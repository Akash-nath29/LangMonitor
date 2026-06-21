from __future__ import annotations

from fastapi import APIRouter

from langmonitor.engine.core import get_main_engine
from langmonitor.schemas.api import (
    ABTestIn,
    ABTestOut,
    InjectStateIn,
    ResumeIn,
    err,
    ok,
)

router = APIRouter(tags=["control"])


@router.post("/runs/{run_id}/kill")
async def kill_run(run_id: str):
    main = get_main_engine()
    res = await main.control.kill_run(run_id)
    return ok(res)


@router.post("/runs/{run_id}/pause")
async def pause_run(run_id: str):
    main = get_main_engine()
    res = await main.control.pause_run(run_id)
    return ok(res)


@router.post("/runs/{run_id}/resume")
async def resume_run(run_id: str, body: ResumeIn | None = None):
    main = get_main_engine()
    patch = body.state_patch if body else None
    res = await main.control.resume_run(run_id, injected_state=patch)
    return ok(res)


@router.post("/runs/{run_id}/inject-state")
async def inject_state(run_id: str, body: InjectStateIn):
    main = get_main_engine()
    res = await main.control.inject_state(run_id, body.patch)
    return ok(res)


@router.get("/ab-tests")
async def list_ab_tests():
    main = get_main_engine()
    tests = await main.control.list_ab_tests()
    return ok([ABTestOut.model_validate(t).model_dump(mode="json") for t in tests])


@router.post("/ab-tests")
async def create_ab_test(body: ABTestIn):
    main = get_main_engine()
    test = await main.control.create_ab_test(
        node_name=body.node_name,
        prompt_a=body.prompt_a,
        prompt_b=body.prompt_b,
        run_id=body.run_id,
    )
    return ok(ABTestOut.model_validate(test).model_dump(mode="json"))


@router.post("/ab-tests/{ab_test_id}/swap")
async def swap_ab_variant(ab_test_id: str):
    main = get_main_engine()
    test = await main.control.swap_ab_variant(ab_test_id)
    if test is None:
        return err("ab test not found")
    return ok(ABTestOut.model_validate(test).model_dump(mode="json"))
