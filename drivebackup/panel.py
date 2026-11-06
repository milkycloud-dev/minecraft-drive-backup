import asyncio
import io
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import discord

from . import engine

log = logging.getLogger("drive-backup")

CATEGORY = os.environ.get("PANEL_CATEGORY", "Backups")
PANEL_NAME = os.environ.get("PANEL_CHANNEL", "backup-panel")
LOG_NAME = os.environ.get("LOG_CHANNEL", "backup-log")
SCHEDULE_ENABLED = os.environ.get("SCHEDULE_ENABLED", "1") == "1"
SCHEDULE_TZ = ZoneInfo(os.environ.get("SCHEDULE_TZ", "UTC"))
SCHEDULE_HOUR, SCHEDULE_MINUTE = (int(x) for x in os.environ.get("SCHEDULE_TIME", "04:00").split(":"))
CATCHUP_HOURS = int(os.environ.get("CATCHUP_HOURS", "6"))
GREEN, BLUE, AMBER, RED, GREY = 0x3BA55D, 0x4F7CFF, 0xF0B232, 0xED4245, 0x80848E
JOB_NAME = os.environ.get("JOB_NAME", "Minecraft server")
STATE_TEXT = {"ok": "ok", "degraded": "with warnings", "failed": "failed", "running": "running"}
STATE_COLOR = {"ok": GREEN, "degraded": AMBER, "failed": RED, "running": BLUE}
STAGE_TEXT = {"peer": "waiting for the other backup", "preflight": "checking Drive and free space", "save": "saving the world",
              "walk": "listing files", "parts": "world and server files", "save2": "saving again", "walk2": "listing again",
              "delta": "files changed during the copy", "finalize": "finishing", "retention": "removing old copies"}
WORKER_STALE = int(os.environ.get("WORKER_STALE", "300"))


def need(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit("%s is not set" % name)
    return value


def human(n):
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return ("%.0f %s" if unit == "B" else "%.1f %s") % (n, unit)
        n /= 1024


def parse_dt(value):
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def stamp_of(dt):
    return "<t:%d:f>" % int(dt.timestamp()) if dt else "?"


def duration(sec):
    sec = int(sec or 0)
    h, rest = divmod(sec, 3600)
    m, s = divmod(rest, 60)
    return "%d h %02d min" % (h, m) if h else "%d min %02d s" % (m, s)


def slot_for(day):
    return datetime(day.year, day.month, day.day, SCHEDULE_HOUR, SCHEDULE_MINUTE, tzinfo=SCHEDULE_TZ)


def next_slot(when):
    local = when.astimezone(SCHEDULE_TZ)
    slot = slot_for(local)
    return slot if slot > local else slot_for(local + timedelta(days=1))


def missed_slot(when, last_started):
    local = when.astimezone(SCHEDULE_TZ)
    slot = slot_for(local)
    if slot > local or local - slot > timedelta(hours=CATCHUP_HOURS):
        return False
    return last_started is None or last_started < slot


def progress_text(cur):
    stage = cur.get("stage") or ""
    text = STAGE_TEXT.get(stage, stage)
    total, read = cur.get("total_bytes") or 0, cur.get("read_bytes") or 0
    if total:
        pct = min(100.0, 100.0 * read / total)
        text += ", %.1f%%: read %s of %s, uploaded %s" % (pct, human(read), human(total), human(cur.get("up_bytes")))
    if cur.get("rate"):
        text += ", %s/s" % human(cur["rate"])
    if cur.get("eta"):
        text += ", about %s left" % duration(cur["eta"])
    return text


class State:
    def __init__(self, path):
        self.path = path
        try:
            with open(path, encoding="utf-8") as f:
                self.data = json.load(f)
        except (FileNotFoundError, ValueError):
            self.data = {}

    def get(self, key, default=None):
        return self.data.get(key, default)

    def update(self, **kw):
        self.data.update(kw)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False)
        os.replace(tmp, self.path)


class Worker:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.heart = os.path.join(data_dir, "worker.heartbeat")
        self.request = os.path.join(data_dir, "run.request")

    def alive(self):
        try:
            return time.time() - os.path.getmtime(self.heart) < WORKER_STALE
        except OSError:
            return False

    def running(self):
        if os.path.exists(self.request):
            return True
        try:
            with open(self.heart) as f:
                return f.read().strip() == "1" and self.alive()
        except OSError:
            return False

    async def start(self):
        if self.running():
            return "busy"
        if not self.alive():
            return "down"
        tmp = self.request + ".tmp"
        with open(tmp, "w") as f:
            f.write(str(int(time.time())))
        os.replace(tmp, self.request)
        return "started"

    def snapshot(self):
        status = engine.Status().load()
        history = []
        try:
            with open(os.path.join(self.data_dir, "history.jsonl"), encoding="utf-8") as f:
                history = [json.loads(line) for line in f.read().splitlines()[-15:] if line]
        except (FileNotFoundError, ValueError):
            pass
        return {"running": self.running(), "alive": self.alive(), "status": status or None, "history": history}

    def log_text(self):
        try:
            with open(os.path.join(self.data_dir, "last.log"), encoding="utf-8", errors="replace") as f:
                return "\n".join(f.read().splitlines()[-400:])
        except FileNotFoundError:
            return ""


class App:
    def __init__(self):
        self.token = need("DISCORD_BOT_TOKEN")
        self.guild_id = int(need("DISCORD_GUILD_ID"))
        self.admin_ids = tuple(int(x) for x in os.environ.get("ADMIN_IDS", "").replace(" ", "").split(",") if x)
        self.data_dir = engine.STATE
        self.state = State(os.path.join(self.data_dir, "panel.json"))
        self.worker = Worker(self.data_dir)
        self.client = discord.Client(intents=discord.Intents.default())
        self.client.setup_hook = self.setup_hook
        self.client.on_ready = self.on_ready
        self.guild = None
        self.owner_id = None
        self.ready_once = False
        self.snap = None
        self.panel_sig = None
        self.wake = asyncio.Event()

    async def setup_hook(self):
        self.client.add_view(PanelView(self))

    async def on_ready(self):
        if self.ready_once:
            return
        self.ready_once = True
        self.guild = self.client.get_guild(self.guild_id) or await self.client.fetch_guild(self.guild_id)
        self.owner_id = self.guild.owner_id
        await self.ensure_channels()
        asyncio.create_task(self.poller())
        asyncio.create_task(self.scheduler())
        asyncio.create_task(self.heartbeat())
        log.info("ready: guild=%s owner=%s", self.guild.name, self.owner_id)

    async def ensure_channels(self):
        guild = self.guild
        owner = guild.get_member(self.owner_id) or await guild.fetch_member(self.owner_id)
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, embed_links=True,
                                                  attach_files=True, manage_messages=True, read_message_history=True),
            owner: discord.PermissionOverwrite(view_channel=True, read_message_history=True),
        }
        for uid in self.admin_ids:
            m = guild.get_member(uid) or await guild.fetch_member(uid)
            overwrites[m] = discord.PermissionOverwrite(view_channel=True, read_message_history=True)
        cat = guild.get_channel(self.state.get("category_id") or 0) or discord.utils.get(guild.categories, name=CATEGORY)
        if cat is None:
            cat = await guild.create_category(CATEGORY, overwrites=overwrites, reason="drive-backup")
        panel = guild.get_channel(self.state.get("panel_channel_id") or 0) or discord.utils.get(
            guild.text_channels, name=PANEL_NAME, category=cat)
        if panel is None:
            panel = await guild.create_text_channel(
                PANEL_NAME, category=cat, overwrites=overwrites,
                topic="Daily backups to Google Drive", reason="drive-backup")
        logch = guild.get_channel(self.state.get("log_channel_id") or 0) or discord.utils.get(
            guild.text_channels, name=LOG_NAME, category=cat)
        if logch is None:
            logch = await guild.create_text_channel(
                LOG_NAME, category=cat, overwrites=overwrites, topic="Backup log",
                reason="drive-backup")
        self.state.update(category_id=cat.id, panel_channel_id=panel.id, log_channel_id=logch.id)

    def allowed(self, user_id):
        return user_id == self.owner_id or user_id in self.admin_ids

    def embed(self, title, color, text="", fields=()):
        e = discord.Embed(title=title, description=text or None, color=color, timestamp=datetime.now(timezone.utc))
        for name, value in fields:
            e.add_field(name=name, value=value[:1024], inline=False)
        return e

    async def post(self, embed, ping=False):
        ch = self.guild.get_channel(self.state.get("log_channel_id"))
        content = "<@%d>" % self.owner_id if ping else None
        await ch.send(content=content, embed=embed,
                      allowed_mentions=discord.AllowedMentions(users=True) if ping else discord.AllowedMentions.none())

    def items_text(self, cur):
        lines = ["`%s` %s" % (i["name"], human(i["size"])) for i in cur.get("items") or []]
        text = "\n".join(lines)
        return text if len(text) <= 1000 else text[:990] + "\n…"

    def result_embed(self, cur):
        state = cur.get("state")
        text = "Started %s, took %s, total %s.\nFolder: `%s`" % (
            stamp_of(parse_dt(cur.get("started"))), duration(cur.get("seconds")), human(cur.get("size")),
            cur.get("path", "?"))
        fields = []
        if cur.get("items"):
            fields.append(("Files (md5 checked against Google Drive)", self.items_text(cur)))
        if cur.get("problems"):
            fields.append(("Warnings", "\n".join("- %s" % p for p in cur["problems"])))
        if cur.get("notes"):
            fields.append(("Notes", "\n".join("- %s" % p for p in cur["notes"])))
        if cur.get("error"):
            fields.append(("Error", cur["error"]))
        if cur.get("kept"):
            fields.append(("On Drive", "%d copies, %s" % (len(cur["kept"]), human(sum(k["size"] for k in cur["kept"])))))
        title = {"ok": "Backup finished", "degraded": "Backup finished with warnings", "failed": "Backup failed"}.get(
            state, "Backup")
        return self.embed(title, STATE_COLOR.get(state, GREY), text, fields)

    async def handle(self, snap):
        cur = snap.get("status") or {}
        stamp = cur.get("stamp")
        state = cur.get("state")
        if self.state.get("reported") is None:
            self.state.update(reported=stamp or "", announced=stamp or "")
            return
        if state == "running" and stamp and stamp != self.state.get("announced"):
            who = self.state.get("manual_by")
            how = "by <@%d>" % who if who else "on schedule"
            self.state.update(announced=stamp)
            await self.post(self.embed("Backup started", BLUE, "Started %s." % how))
        if state in ("ok", "degraded", "failed") and stamp != self.state.get("reported"):
            self.state.update(reported=stamp, announced=stamp, manual_by=None)
            await self.post(self.result_embed(cur), ping=state != "ok")
        if snap.get("alive"):
            if self.state.get("worker_alert"):
                self.state.update(worker_alert=False)
        elif not self.state.get("worker_alert"):
            self.state.update(worker_alert=True)
            await self.post(self.embed("Backup worker is not answering", RED,
                                       "Check the drive-backup-worker container."), ping=True)
        started = parse_dt(cur.get("started"))
        if SCHEDULE_ENABLED and started and not snap.get("running"):
            if datetime.now(timezone.utc) - started > timedelta(hours=26) and self.state.get("missed_alert") != stamp:
                self.state.update(missed_alert=stamp)
                await self.post(self.embed("No backup for over a day", RED,
                                           "Last run: %s. Check the drive-backup containers." %
                                           stamp_of(started)), ping=True)

    def panel_embed(self):
        e = discord.Embed(title="Backups: %s" % JOB_NAME)
        e.description = ("Every day at %02d:%02d %s. Data is compressed on the fly and streamed straight to Google Drive, "
                         "nothing is stored on disk. The %d newest copies are kept." % (
                             SCHEDULE_HOUR, SCHEDULE_MINUTE, SCHEDULE_TZ.key, engine.KEEP))
        snap = self.snap or {}
        cur = snap.get("status") or {}
        if self.snap and not snap.get("alive"):
            e.color = RED
            e.add_field(name="Worker", value="not answering: check drive-backup-worker", inline=False)
        elif snap.get("running") or cur.get("state") == "running":
            e.color = BLUE
            e.add_field(name="Now", value="backing up: %s" % progress_text(cur), inline=False)
        else:
            e.color = STATE_COLOR.get(cur.get("state"), GREY)
        last = cur if cur.get("state") in ("ok", "degraded", "failed") else None
        if not last:
            done = [h for h in snap.get("history") or [] if h.get("state") in ("ok", "degraded", "failed")]
            last = done[-1] if done else None
        if last:
            e.add_field(name="Last", value="%s: %s, %s, %s" % (
                stamp_of(parse_dt(last.get("started"))), STATE_TEXT.get(last.get("state"), last.get("state")),
                human(last.get("size")), duration(last.get("seconds"))), inline=False)
            if last.get("problems") or last.get("error"):
                text = "\n".join("- %s" % p for p in (last.get("problems") or []))
                if last.get("error"):
                    text += "\n- %s" % last["error"]
                e.add_field(name="Warnings", value=text.strip()[:1024], inline=False)
        else:
            e.add_field(name="Last", value="none yet", inline=False)
        if SCHEDULE_ENABLED:
            ts = int(next_slot(datetime.now(timezone.utc)).timestamp())
            e.add_field(name="Next", value="<t:%d:f> (<t:%d:R>)" % (ts, ts))
        else:
            e.add_field(name="Next", value="schedule is off")
        kept = (last or {}).get("kept") or []
        if kept:
            e.add_field(name="On Drive", value="%d copies, %s" % (len(kept), human(sum(k["size"] for k in kept))))
        e.set_footer(text="Updated")
        e.timestamp = datetime.now(timezone.utc)
        return e

    async def refresh_panel(self, force=False):
        embed = self.panel_embed()
        sig = json.dumps({k: v for k, v in embed.to_dict().items() if k != "timestamp"}, sort_keys=True)
        if sig == self.panel_sig and not force:
            return
        ch = self.guild.get_channel(self.state.get("panel_channel_id"))
        mid = self.state.get("panel_message_id")
        if mid:
            try:
                msg = await ch.fetch_message(mid)
                await msg.edit(embed=embed, view=PanelView(self))
                self.panel_sig = sig
                return
            except discord.NotFound:
                pass
        msg = await ch.send(embed=embed, view=PanelView(self))
        self.state.update(panel_message_id=msg.id)
        self.panel_sig = sig

    async def poll_once(self):
        self.snap = await asyncio.to_thread(self.worker.snapshot)
        await self.handle(self.snap)
        await self.refresh_panel()

    async def poller(self):
        while True:
            try:
                await self.poll_once()
            except Exception:
                log.exception("poll step failed")
            running = bool(self.snap) and self.snap.get("running")
            self.wake.clear()
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=20 if running else 120)
            except asyncio.TimeoutError:
                pass

    async def scheduler(self):
        if not SCHEDULE_ENABLED:
            return
        current = engine.Status().load()
        if missed_slot(datetime.now(timezone.utc), parse_dt(current.get("started"))):
            log.info("catching up a missed slot")
            await self.worker.start()
            self.wake.set()
        target = next_slot(datetime.now(timezone.utc))
        while True:
            await asyncio.sleep(30)
            if datetime.now(timezone.utc) >= target:
                out = await self.worker.start()
                log.info("scheduled start: %s", out)
                self.wake.set()
                target = next_slot(datetime.now(timezone.utc))

    async def heartbeat(self):
        path = os.path.join(self.data_dir, "heartbeat")
        while True:
            with open(path, "w") as f:
                f.write(str(int(datetime.now().timestamp())))
            await asyncio.sleep(30)

    def run(self):
        self.client.run(self.token, log_handler=None)


class PanelView(discord.ui.View):
    def __init__(self, app):
        super().__init__(timeout=None)
        self.app = app

    async def interaction_check(self, interaction):
        if self.app.allowed(interaction.user.id):
            return True
        await interaction.response.send_message("No access.", ephemeral=True)
        return False

    @discord.ui.button(label="Refresh", emoji="🔄", style=discord.ButtonStyle.primary, custom_id="dbk:refresh")
    async def refresh(self, interaction, button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.app.poll_once()
        await self.app.refresh_panel(force=True)
        await interaction.followup.send("Updated.", ephemeral=True)

    @discord.ui.button(label="Run now", emoji="▶️", style=discord.ButtonStyle.success, custom_id="dbk:run")
    async def run_now(self, interaction, button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        out = await self.app.worker.start()
        if out == "started":
            self.app.state.update(manual_by=interaction.user.id)
            await interaction.followup.send("Backup started. Progress is in the panel, the result goes to #%s." % LOG_NAME,
                                            ephemeral=True)
        elif out == "down":
            await interaction.followup.send("The worker is not answering, the run was not accepted.", ephemeral=True)
        else:
            await interaction.followup.send("A backup is already running.", ephemeral=True)
        self.app.wake.set()

    @discord.ui.button(label="Copies", emoji="📋", style=discord.ButtonStyle.secondary, custom_id="dbk:list")
    async def list_backups(self, interaction, button):
        snap = self.app.snap or {}
        done = [h for h in snap.get("history") or [] if h.get("kept")]
        kept = (snap.get("status") or {}).get("kept") or (done[-1]["kept"] if done else [])
        if not kept:
            await interaction.response.send_message("No copies yet.", ephemeral=True)
            return
        lines = ["`%s` %s" % (k["name"], human(k["size"])) for k in reversed(kept)]
        e = self.app.embed("Copies on Google Drive", BLUE, "\n".join(lines))
        await interaction.response.send_message(embed=e, ephemeral=True)

    @discord.ui.button(label="Log", emoji="📜", style=discord.ButtonStyle.secondary, custom_id="dbk:log")
    async def last_log(self, interaction, button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        text = await asyncio.to_thread(self.app.worker.log_text)
        if not text.strip():
            await interaction.followup.send("The log is empty.", ephemeral=True)
            return
        f = discord.File(io.BytesIO(text.encode("utf-8")), filename="drive-backup-last.log")
        await interaction.followup.send("Log of the last run.", file=f, ephemeral=True)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    App().run()


if __name__ == "__main__":
    main()
