from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict

app = FastAPI()

registry: Dict[str, List[str]] = {}


class ServiceRegistration(BaseModel):
    service_name: str
    url: str


@app.post("/register")
def register_service(reg: ServiceRegistration):
    if reg.service_name not in registry:
        registry[reg.service_name] = []
    if reg.url not in registry[reg.service_name]:
        registry[reg.service_name].append(reg.url)
    print(f"[config-server] Registered '{reg.service_name}' at {reg.url}")
    print(f"[config-server] Current registry: {registry}")
    return {"status": "registered", "service_name": reg.service_name, "url": reg.url}


@app.get("/services/{service_name}")
def get_service_urls(service_name: str):
    urls = registry.get(service_name, [])
    print(f"[config-server] Request for '{service_name}' -> {urls}")
    return {"service_name": service_name, "urls": urls}


@app.get("/registry")
def get_full_registry():
    return registry
