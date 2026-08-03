#!/usr/bin/env python3
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles
from dateutil.parser import isoparse
import uvicorn
import docker
import time
import asyncio
import os

from jinja2 import Environment, FileSystemLoader, select_autoescape

from typing import List
import re

scraped = None

exporter_mode = os.environ.get("EXPORTER_MODE", "docker")
scrape_delay = float(os.environ.get("SCRAPE_DELAY", "10"))

if exporter_mode == "swarm":
    status_list=["new", "pending", "assigned", "accepted", "ready", "preparing", "starting", "running", "complete", "failed", "shutdown", "rejected", "orphaned", "remove"]
else:
    status_list=["created", "restarting", "running", "removing", "paused", "exited", "dead"]

env = Environment(
    loader=FileSystemLoader('./templates'),
    autoescape=select_autoescape(['j2'])
)
template = env.get_template(exporter_mode + '_metrics.j2')

tasks_cache = {}

async def scrape_containers_info():
    while True:
        global scraped
        if exporter_mode == "swarm":
           scraped = await get_services()
        else:
           scraped = await get_containers()
        await asyncio.sleep(scrape_delay)

app = FastAPI()

async def get_docker_client():
    retries = 5
    backoff = 2

    for attempt in range(retries):
        try:
            return docker.from_env()
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(backoff)
                backoff *= 2
                print(f"Reconnect attempt № {attempt + 1}")
                continue
            else:
                raise RuntimeError(f"Failed to connect to Docker after {retries} attempts: {e}. The container needs to be restarted!")

async def get_containers():
    cli = await get_docker_client()
    t = []
    try:
        for container in cli.containers.list(all=True,ignore_removed=True):
            t.append({
                "image": container.attrs['Config']['Image'],
                "name": container.name,
                "status": container.status,
                "time": int(time.time())
            })
    except Exception as e:
        print(f"Error receiving containers: {e}")
    finally:
        cli.close()
    return(t)

async def update_tasks(service):
    global tasks_cache
    if service.name not in tasks_cache:
        tasks_cache[service.name] = {
        "tasks": [],
        "timestamps": {
            "new_timestamp": 0,
            "pending_timestamp": 0,
            "assigned_timestamp": 0,
            "accepted_timestamp": 0,
            "ready_timestamp": 0,
            "preparing_timestamp": 0,
            "starting_timestamp": 0,
            "running_timestamp": 0,
            "complete_timestamp": 0,
            "failed_timestamp": 0,
            "shutdown_timestamp": 0,
            "rejected_timestamp": 0,
            "orphaned_timestamp": 0,
            "remove_timestamp": 0,
        }
    }

    t = []
    for task in service.tasks():
        timestamp = isoparse(task['Status']['Timestamp']).timestamp()
        t.append({
            "id": task['ID'],
            "status": task['Status']['State'],
            "latest_timestamp": timestamp
        })
        status_timestamp_name = f"{task['Status']['State']}_timestamp"
        if tasks_cache[service.name]["timestamps"][status_timestamp_name] < timestamp:
            tasks_cache[service.name]["timestamps"][status_timestamp_name] = timestamp

    if len(t) != 0:
        tasks_cache[service.name]["tasks"] = t 
    else:
        tasks_cache[service.name]["tasks"] = [{
            "id": "None",
            "status": "None",
            "latest_timestamp": "None"
        }]

async def sync_tasks_cache(services):
    services = [s.name for s in services]
    global tasks_cache
    for service in tasks_cache.copy():
        if service not in services:
            tasks_cache.pop(service)

async def get_services():
    cli = await get_docker_client()
    s = []
    try:
        await sync_tasks_cache(cli.services.list())
        for service in cli.services.list():
            service_inspect = service.attrs
            if 'Replicated' in service_inspect["Spec"]["Mode"]:
                replicas = service_inspect["Spec"]["Mode"]["Replicated"]["Replicas"]
            else: replicas = -1
            await update_tasks(service=service)
            s.append({
                "name": service.name,
                "image": service_inspect['Spec']['TaskTemplate']['ContainerSpec']['Image'],
                "replicas": replicas,
                "tasks": tasks_cache[service.name]["tasks"],
                "tasks_timestamps": tasks_cache[service.name]["timestamps"],
                "time": int(time.time())
            })
    except Exception as e:
        print(f"Error receiving services: {e}")
    finally:
        cli.close()
    return(s)

def renderer(scraped):
    if exporter_mode == "swarm":
        return(template.render(
            services=scraped,
            statuses=status_list
        ))
    else:
        return(template.render(
            containers=scraped,
            statuses=status_list
        ))

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(scrape_containers_info())

@app.get('/metrics')
async def metrics():
    return PlainTextResponse(content=renderer(scraped), status_code=200)

@app.get("/probe")
async def probe(
        include: str = '.*',
        exclude: str = ''):
    containers = []
    for container in scraped:
        if re.fullmatch(include,container['name']) and not re.fullmatch(exclude,container['name']):
            containers.append(container)
    return PlainTextResponse(content=renderer(containers), status_code=200)

app.mount("/", StaticFiles(directory="./static", html="True"), name="static")

if __name__ == "__main__":
    uvicorn.run("main:app", port=8000, host='0.0.0.0', reload='True')
