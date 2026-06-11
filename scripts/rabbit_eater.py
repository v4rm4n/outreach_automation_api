#!/usr/bin/env python3
import asyncio
import yaml
import typer
import sys
import os
import urllib.parse
import json
import base64
from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.prompt import Confirm
from rich import box
from rich.panel import Panel
from rich.syntax import Syntax
from rich.live import Live
from collections import defaultdict
from aio_pika import connect_robust, IncomingMessage
from aio_pika.abc import AbstractIncomingMessage

# Try importing httpx
try:
    import httpx
except ImportError:
    httpx = None  # type: ignore

HTTPX_AVAILABLE = httpx is not None

# --- Config Import Logic ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from config import STORECFG
    RABBIT_URL = STORECFG["RABBIT_URL"]
except ImportError:
    from dotenv import load_dotenv
    load_dotenv()
    user = os.getenv("RMQ_USER", "guest")
    password = os.getenv("RMQ_PASS", "guest")
    host = os.getenv("RMQ_HOST", "localhost:5672")
    RABBIT_URL = f"amqp://{user}:{password}@{host}/"

# --- State Management ---
state = {
    "topology_path": None,
    "mode": "cluster" # 'cluster' or 'topology'
}

app = typer.Typer(help="RabbitMQ Manager: Manage specific topologies or inspect the whole cluster.")
console = Console()

# --- Helper Functions ---

def get_rabbitmq_url() -> str:
    return RABBIT_URL

def get_management_url(amqp_url: str) -> str:
    """Infers the HTTP Management API URL from the AMQP URL."""
    parsed = urllib.parse.urlparse(amqp_url)
    
    if parsed.port == 5671:
        scheme = "https"
        mgmt_port = 15671
    else:
        scheme = "http"
        mgmt_port = 15672 if (parsed.port is None or parsed.port == 5672) else parsed.port + 10000

    hostname = parsed.hostname or "localhost"
    return f"{scheme}://{hostname}:{mgmt_port}"

def get_vhost(amqp_url: str) -> str:
    parsed = urllib.parse.urlparse(amqp_url)
    vhost = parsed.path.strip('/')
    return vhost if vhost else "/"

def get_auth_tuple(amqp_url: str) -> tuple[str, str]:
    parsed = urllib.parse.urlparse(amqp_url)
    username = parsed.username or "guest"
    password = parsed.password or "guest"
    return (username, password)

def load_topology_queues(topology_path: Path) -> list[str]:
    if not topology_path.exists():
        console.print(f"[bold red]Error:[/] File {topology_path} not found.")
        raise typer.Exit(1)

    with open(topology_path, "r") as f:
        data = yaml.safe_load(f)
    
    queues = []
    if "queues" in data and isinstance(data["queues"], list):
        queues = [q.get("name") for q in data["queues"] if "name" in q]
    
    return queues

def serialize_message(body: bytes, routing_key: str, properties: Any) -> str:
    """Creates a JSON line with Base64 body to safely store binary data."""
    props_dict = {}
    amqp_fields = [
        "headers", "content_type", "content_encoding", "delivery_mode", 
        "priority", "correlation_id", "reply_to", "expiration", 
        "message_id", "timestamp", "type", "user_id", "app_id"
    ]

    if isinstance(properties, dict):
        props_dict = properties
    else:
        for field in amqp_fields:
            if hasattr(properties, field):
                val = getattr(properties, field)
                if val is not None:
                    if hasattr(val, "value"): 
                        val = val.value
                    props_dict[field] = val

    return json.dumps({
        "timestamp": datetime.now().isoformat(),
        "routing_key": routing_key,
        "properties": props_dict,
        "encoding": "base64",
        "body": base64.b64encode(body).decode('utf-8')
    })

# --- Stats & Metadata Fetching ---

async def fetch_full_cluster_state(target_queues: Optional[list[str]] = None) -> Dict[str, Any]:
    """Fetches Queues AND Bindings from RabbitMQ API."""
    if not HTTPX_AVAILABLE:
        console.print("[red]Error: 'httpx' is required for cluster inspection.[/]")
        return {}
    
    assert httpx is not None  # narrows type from Module | None to Module

    amqp_url = get_rabbitmq_url()
    base_url = get_management_url(amqp_url)
    vhost_raw = get_vhost(amqp_url)
    vhost_enc = "%2f" if vhost_raw == "/" else urllib.parse.quote(vhost_raw, safe='')
    auth = get_auth_tuple(amqp_url)

    results = {}

    async with httpx.AsyncClient(timeout=5.0) as client:
        q_resp = await client.get(f"{base_url}/api/queues/{vhost_enc}", auth=auth)
        if q_resp.status_code != 200:
            console.print(f"[red]Failed to fetch queues: {q_resp.status_code}[/]")
            return {}
        
        all_queues_data = q_resp.json()
        b_resp = await client.get(f"{base_url}/api/bindings/{vhost_enc}", auth=auth)
        
        bindings_map = defaultdict(list)
        if b_resp.status_code == 200:
            all_bindings = b_resp.json()
            for b in all_bindings:
                if b.get("destination_type") == "queue":
                    src = b.get("source")
                    if src == "": src = "(AMQP Default)"
                    key = b.get("routing_key")
                    bindings_map[b.get("destination")].append(f"{src} [dim]→[/] [cyan]{key}[/]")

        for q_data in all_queues_data:
            q_name = q_data.get("name")
            if target_queues is not None and q_name not in target_queues:
                continue

            results[q_name] = {
                "ready": q_data.get("messages_ready", 0),
                "total": q_data.get("messages", 0),
                "rate_in": q_data.get("messages_details", {}).get("rate", 0.0),
                "state": q_data.get("state", "unknown"),
                "type": q_data.get("type", "classic"),
                "consumers": q_data.get("consumers", 0),
                "bindings": bindings_map.get(q_name, [])
            }
            
    return results

async def _fetch_queue_bindings_raw(queue_name: str) -> List[Dict]:
    """Helper to get raw bindings for Wiretap setup."""
    if not HTTPX_AVAILABLE: return []

    assert httpx is not None  # narrows type from Module | None to Module

    amqp_url = get_rabbitmq_url()
    base_url = get_management_url(amqp_url)
    vhost_raw = get_vhost(amqp_url)
    vhost_enc = "%2f" if vhost_raw == "/" else urllib.parse.quote(vhost_raw, safe='')
    auth = get_auth_tuple(amqp_url)

    async with httpx.AsyncClient(timeout=5.0) as client:
        # We fetch ALL bindings (API doesn't filter by destination easily) and filter locally
        resp = await client.get(f"{base_url}/api/bindings/{vhost_enc}", auth=auth)
        if resp.status_code != 200: return []
        
        all_bindings = resp.json()
        target_bindings = []
        for b in all_bindings:
            if b.get("destination_type") == "queue" and b.get("destination") == queue_name:
                # Ignore default exchange binding (cannot bind to it)
                if b.get("source") == "": continue
                target_bindings.append({
                    "source": b.get("source"),
                    "routing_key": b.get("routing_key")
                })
        return target_bindings

# --- Action Functions ---

async def _purge_queue(queue_name: str, connection) -> int:
    async with connection.channel() as channel:
        try:
            queue = await channel.get_queue(queue_name, ensure=False)
            purge_result = await queue.purge()
            return purge_result.message_count
        except Exception:
            return 0

async def _consume_n_messages(queue_name: str, count: int, connection) -> int:
    eaten = 0
    async with connection.channel() as channel:
        try:
            queue = await channel.get_queue(queue_name, ensure=False)
            for _ in range(count):
                msg = await queue.get(fail=False)
                if msg is None: break
                async with msg.process():
                    eaten += 1
        except Exception:
            pass
    return eaten

async def _dump_queue_amqp(queue_name: str, count: int, filename: str):
    url = get_rabbitmq_url()
    try:
        connection = await connect_robust(url)
    except Exception as e:
        console.print(f"[red]Connection failed: {e}[/]")
        return
    
    saved_count = 0
    async with connection:
        async with connection.channel() as channel:
            try:
                queue = await channel.get_queue(queue_name)
            except Exception:
                console.print(f"[red]Queue {queue_name} not found.[/]")
                return
            
            with open(filename, "a", encoding="utf-8") as f:
                with console.status(f"[red]Siphoning {count} msgs from {queue_name}...[/]"):
                    for _ in range(count):
                        msg: AbstractIncomingMessage | None = await queue.get(fail=False)
                        if msg is None: break
                        async with msg.process(): 
                            line = serialize_message(msg.body, msg.routing_key or "", msg)
                            f.write(line + "\n")
                            saved_count += 1
    console.print(f"[green]Dumped {saved_count} messages to {filename}[/]")

async def _copy_queue_http(queue_name: str, count: int, filename: str):
    if not HTTPX_AVAILABLE: 
        console.print("[red]Error: 'httpx' is required.[/]")
        return
    
    assert httpx is not None  # narrows type from Module | None to Module

    amqp_url = get_rabbitmq_url()
    base_url = get_management_url(amqp_url)
    vhost = urllib.parse.quote(get_vhost(amqp_url), safe='')
    auth = get_auth_tuple(amqp_url)

    payload = {
        "count": count,
        "ackmode": "ack_requeue_true", 
        "encoding": "base64"
    }

    console.print(f"[cyan]Copying (Peeking) {count} msgs from {queue_name} via API...[/]")
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{base_url}/api/queues/{vhost}/{queue_name}/get", 
            json=payload, auth=auth
        )
        if resp.status_code != 200:
            console.print(f"[red]API Error: {resp.status_code} - {resp.text}[/]")
            return

        messages = resp.json()
        saved_count = 0
        with open(filename, "a", encoding="utf-8") as f:
            for msg in messages:
                body_bytes = base64.b64decode(msg.get("payload")) if msg.get("payload_encoding") == "base64" else msg.get("payload").encode()
                line = serialize_message(
                    body_bytes, 
                    msg.get("routing_key"), 
                    msg.get("properties", {})
                )
                f.write(line + "\n")
                saved_count += 1
    console.print(f"[green]Copied {saved_count} messages to {filename}[/]")

async def _inspect_live_traffic(queue_name: str):
    """Wiretap: Creates a temporary queue bound to the same exchanges to spy on traffic."""
    
    # 1. Get Bindings to replicate
    bindings = await _fetch_queue_bindings_raw(queue_name)
    if not bindings:
        console.print(f"[yellow]Warning: No exchange bindings found for {queue_name}.[/]")
        console.print("[dim]If this queue only receives messages via default exchange (direct send), Wiretap cannot spy on it.[/]")
        if not Confirm.ask("Try connecting anyway (might be empty)?"):
            return

    url = get_rabbitmq_url()
    try:
        connection = await connect_robust(url)
    except Exception as e:
        console.print(f"[red]Connection failed: {e}[/]")
        return

    async with connection:
        async with connection.channel() as channel:
            # 2. Create Spy Queue
            # Exclusive=True means it deletes itself when we disconnect
            spy_queue = await channel.declare_queue(exclusive=True, auto_delete=True)
            
            # 3. Replicate Bindings
            console.print(f"[green]Setting up Wiretap on {len(bindings)} bindings...[/]")
            for b in bindings:
                await spy_queue.bind(exchange=b['source'], routing_key=b['routing_key'])
                console.print(f"  [dim]Bound to[/] {b['source']} -> {b['routing_key']}")

            console.print(Panel(f"[bold cyan]Listening for LIVE traffic on {queue_name}...[/]\n[dim]Press CTRL+C to stop.[/]", border_style="cyan"))

            # 4. Stream
            try:
                async with spy_queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        message: AbstractIncomingMessage
                        async with message.process():
                            # Auto-Ack on Spy Queue (doesn't affect real queue)
                            try:
                                body_str = message.body.decode()
                                # Pretty print JSON if possible
                                body_fmt = json.dumps(json.loads(body_str), indent=2)
                                lexer = "json"
                            except:
                                body_fmt = str(message.body)
                                lexer = "text"

                            console.rule(f"[bold magenta]{datetime.now().strftime('%H:%M:%S')}[/] - {message.routing_key}")
                            console.print(Syntax(body_fmt, lexer, theme="monokai", word_wrap=True))
                            
            except asyncio.CancelledError:
                pass
            except KeyboardInterrupt:
                pass
            
    console.print("\n[yellow]Wiretap removed. Exiting.[/]")

# --- CLI COMMANDS ---

@app.callback()
def main(
    topology: Optional[Path] = typer.Option(None, "--topology", "-t", help="Filter by topology file.")
):
    if topology:
        state["topology_path"] = topology
        state["mode"] = "topology"
    else:
        state["topology_path"] = None
        state["mode"] = "cluster"

def generate_table(data: Dict[str, Any], show_all: bool = False) -> Table:
    """Helper to generate the table object with filtering."""
    table = Table(title=f"RabbitMQ Inspector", box=box.ROUNDED, expand=True)
    table.add_column("Queue", style="bold magenta")
    table.add_column("State", justify="center")
    table.add_column("Ready", justify="right", style="green")
    table.add_column("Rate", justify="right", style="cyan")
    table.add_column("Bindings", style="white")

    if not data:
        return table

    visible_count = 0
    total_count = len(data)

    for q_name, stats in sorted(data.items()):
        is_active = stats["ready"] > 0 or stats["rate_in"] > 0
        
        # SKIP if we aren't showing all AND the queue is idle
        if not show_all and not is_active:
            continue

        visible_count += 1
        
        state_style = "green" if stats["state"] == "running" else "red"
        
        # Compact bindings for cleaner view
        bind_list = stats["bindings"]
        if len(bind_list) > 3:
            bind_str = "\n".join(bind_list[:2]) + f"\n[dim]+ {len(bind_list)-2} more...[/]"
        else:
            bind_str = "\n".join(bind_list) if bind_list else "[dim]-[/]"

        rate_str = f"{stats['rate_in']:.1f}/s" if stats['rate_in'] > 0 else "-"

        table.add_row(
            q_name,
            f"[{state_style}]{stats['state']}[/]",
            str(stats["ready"]),
            rate_str,
            bind_str
        )

    # Add a caption so you know things are hidden
    if visible_count < total_count:
        table.caption = f"[dim]Showing {visible_count} of {total_count} queues. Use --all to see inactive.[/]"
    
    return table

@app.command(name="list")
def list_queues(
    watch: bool = typer.Option(False, "--watch", "-w", help="Refresh the table continuously."),
    interval: float = typer.Option(2.0, "--interval", "-i", help="Seconds between updates in watch mode."),
    show_all: bool = typer.Option(False, "--all", "-a", help="Show all queues, even empty ones (default: False in watch mode).")
):
    """List queues. In watch mode, empty queues are hidden by default to fit the screen."""
    target_queues = load_topology_queues(state["topology_path"]) if state["mode"] == "topology" else None

    # Logic: If user didn't explicitly ask for --all, default to False (hidden)
    # However, for a single static 'list' command, we usually want to see everything unless specified.
    # Let's trust the flag passed by the user.
    
    async def run_list():
        if watch:
            # --- WATCH MODE ---
            # Initial render
            with Live(generate_table({}, show_all=show_all), refresh_per_second=4, screen=True) as live:
                while True:
                    try:
                        data = await fetch_full_cluster_state(target_queues)
                        live.update(generate_table(data, show_all=show_all))
                        await asyncio.sleep(interval)
                    except asyncio.CancelledError:
                        break
                    except KeyboardInterrupt:
                        break
        else:
            # --- SINGLE RUN MODE ---
            with console.status(f"[bold green]Scanning...[/]"):
                data = await fetch_full_cluster_state(target_queues)
            
            if not data:
                console.print("[yellow]No queues found.[/]")
                return
            
            # For static print, force show_all=True unless user specifically filters? 
            # Actually, respecting the flag is better. But standard 'ls' should probably show all.
            # If you want default behavior:
            final_show_all = show_all or (not watch and not show_all) 
            
            console.print(generate_table(data, show_all=final_show_all))

    try:
        asyncio.run(run_list())
    except KeyboardInterrupt:
        pass

@app.command()
def flush_all(force: bool = typer.Option(False, "--force", "-f")):
    """Flush queues."""
    target_queues = []
    if state["mode"] == "topology":
        target_queues = load_topology_queues(state["topology_path"])
        mode_desc = f"Topology ({len(target_queues)} queues)"
    else:
        console.print("[dim]Fetching list of all queues...[/]")
        data = asyncio.run(fetch_full_cluster_state(None))
        target_queues = list(data.keys())
        mode_desc = f"ENTIRE CLUSTER ({len(target_queues)} queues)"

    if not target_queues:
        console.print("[yellow]No queues found.[/]")
        return

    if not force:
        color = "bold red" if state["mode"] == "cluster" else "bold yellow"
        console.print(f"\nTargeting: [{color}]{mode_desc}[/]")
        if state["mode"] == "cluster":
            console.print("[bold red blink]WARNING: FLUSHING ALL QUEUES![/]")
        if not Confirm.ask(f"Purge {len(target_queues)} queues?"):
            raise typer.Abort()

    async def run():
        url = get_rabbitmq_url()
        try: connection = await connect_robust(url)
        except Exception as e: 
            console.print(f"[red]Connection failed: {e}[/]"); return
        async with connection:
            table = Table(title="Flush Results")
            table.add_column("Queue", style="cyan"); table.add_column("Purged", style="green")
            with console.status(f"[bold red]Purging...[/]"):
                for q in target_queues:
                    count = await _purge_queue(q, connection)
                    table.add_row(q, str(count))
            console.print(table)
    asyncio.run(run())

@app.command()
def flush(queue_name: str, count: Optional[int] = typer.Option(None, "--count", "-c")):
    """Flush specific queue."""
    async def run():
        url = get_rabbitmq_url()
        connection = await connect_robust(url)
        async with connection:
            if count:
                n = await _consume_n_messages(queue_name, count, connection)
                console.print(f"[green]Ate {n} messages from {queue_name}.[/]")
            else:
                n = await _purge_queue(queue_name, connection)
                console.print(f"[green]Purged {n} messages from {queue_name}.[/]")
    asyncio.run(run())

@app.command()
def dump(queue_name: str, count: int = 100, out: str = typer.Option("dump.jsonl")):
    """[Destructive] Siphon messages to file."""
    if Confirm.ask(f"REMOVE {count} messages from {queue_name}?"):
        asyncio.run(_dump_queue_amqp(queue_name, count, out))

@app.command()
def copy(queue_name: str, count: int = 100, out: str = typer.Option("copy.jsonl")):
    """[Safe] Copy messages to file."""
    asyncio.run(_copy_queue_http(queue_name, count, out))

@app.command()
def inspect(queue_name: str):
    """[Safe] Live Tail/Spy on incoming messages to stdout."""
    asyncio.run(_inspect_live_traffic(queue_name))

@app.command()
def pending():
    """Track delayed messages waiting to be dispatched (via MongoDB Source of Truth)."""
    try:
        from pymongo import MongoClient
        import os
        from datetime import datetime, timezone
        
        # Connect to your Mongo instance
        # Ensure this matches your actual DB name!
        mongo_url = os.getenv("MONGO_URL", "mongodb://localhost:27017")
        client = MongoClient(mongo_url)
        db = client["outreachapi"] # Replace with your actual DB name if different
        
        # Fetch pending jobs sorted by their scheduled date
        jobs = list(db["dispatch_jobs"].find({"status": "pending"}).sort("scheduled_for", 1))
        
        if not jobs:
            console.print("[yellow]No delayed messages currently scheduled.[/]")
            return

        table = Table(title="Scheduled Dispatch Jobs (MongoDB Outbox)", box=box.ROUNDED, expand=True)
        table.add_column("Job ID", style="dim")
        table.add_column("Campaign ID", style="cyan")
        table.add_column("Creator ID", style="magenta")
        table.add_column("Status", style="yellow")
        table.add_column("Priority", justify="center", style="bold red")
        table.add_column("Scheduled For", justify="right", style="green")
        
        now = datetime.now(timezone.utc)
        
        for j in jobs:
            sched = j.get("scheduled_for")
            
            # Calculate time remaining
            if sched:
                # Ensure sched is aware of timezone
                if sched.tzinfo is None:
                    sched = sched.replace(tzinfo=timezone.utc)
                diff = sched - now
                if diff.total_seconds() > 0:
                    time_str = f"{sched.strftime('%b %d, %H:%M:%S')} (in {diff.total_seconds() / 3600:.1f}h)"
                else:
                    time_str = "[bold red]Dispatching Now...[/]"
            else:
                time_str = "Immediate"

            table.add_row(
                str(j.get("_id")),
                j.get("campaign_id")[-6:], # Truncate for cleaner UI
                j.get("creator_id")[-6:],
                j.get("status"),
                str(j.get("priority", 0)),
                time_str
            )
            
        console.print(table)
    except Exception as e:
        console.print(f"[red]Failed to fetch from MongoDB: {e}[/]")

@app.command()
def menu():
    """Interactive Menu."""
    while True:
        mode_str = f"Topology ({state['topology_path']})" if state['mode'] == 'topology' else "Cluster"
        console.print(f"\n[bold green]Rabbit Eater[/] - [dim]{mode_str}[/]")
        console.print("1. List Queues")
        console.print("2. Flush Queue")
        console.print("3. Flush All/Context")
        console.print("4. Dump (Destructive Save)")
        console.print("5. Copy (Safe Save)")
        console.print("6. Inspect (Live Spy)")
        console.print("7. Track Pending/Delayed")
        console.print("8. Exit")
        
        c = typer.prompt("Select")
        if c == "1": list_queues()
        elif c == "2": flush(typer.prompt("Queue"), count=None)
        elif c == "3": 
            try: flush_all(force=False)
            except typer.Abort: console.print("[yellow]Aborted[/]")
        elif c == "4": dump(typer.prompt("Queue"), typer.prompt("Count", type=int), typer.prompt("File", default="dump.jsonl"))
        elif c == "5": copy(typer.prompt("Queue"), typer.prompt("Count", type=int), typer.prompt("File", default="copy.jsonl"))
        elif c == "6": inspect(typer.prompt("Queue"))
        elif c == "7": pending()
        elif c == "8": break

if __name__ == "__main__":
    app()