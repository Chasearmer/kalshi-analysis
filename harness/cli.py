"""CLI entry point for the experiment lab harness."""

from pathlib import Path

import click

from harness.manifest import resolve_run
from harness.run_state import default_state_path, load_state


def _build_execution_overrides(
    *,
    execution_mode: str,
    container_runtime: str | None,
    container_network: str | None,
    use_bypass_permissions: bool | None,
):
    from harness.isolation_launcher import ExecutionOverrides

    mode = None if execution_mode == "auto" else execution_mode
    return ExecutionOverrides(
        mode=mode,
        runtime=container_runtime,
        network=container_network,
        use_bypass_permissions=use_bypass_permissions,
    )


def _state_summary(state) -> str:
    return (
        f"Run stopped: {state.run_id} reason={state.last_stop_reason} "
        f"cost=${state.cumulative_cost_usd:.4f} iterations={state.iteration}"
    )


def _execute_with_selected_mode(*, run_ref, mode: str, cfg, execution_overrides):
    from harness.isolation_launcher import launch_container_worker, resolve_execution_config
    from harness.ralph_loop import run_loop

    execution = resolve_execution_config(run_ref.run_dir, execution_overrides)
    if execution.mode == "container":
        launch_container_worker(
            run_dir=run_ref.run_dir,
            run_id=run_ref.run_id,
            mode=mode,
            loop_cfg=cfg,
            execution=execution,
        )
        state_path = default_state_path(run_ref.run_dir)
        if not state_path.exists():
            raise click.ClickException(
                f"Container worker exited but state file is missing: {state_path}"
            )
        return load_state(state_path)
    return run_loop(run_ref.run_dir, run_ref.run_id, mode=mode, cfg=cfg)


@click.group()
def main():
    """Experiment lab for testing AI research architectures."""


@main.command()
@click.option("--arch", required=True, help="Architecture name (e.g., single_agent)")
@click.option("--problem", required=True, help="Problem name (e.g., kalshi)")
@click.option("--name", required=True, help="Run name (e.g., baseline_test)")
def scaffold(arch: str, problem: str, name: str):
    """Create an isolated run workspace."""
    from harness.scaffold import create_run

    create_run(arch=arch, problem=problem, name=name)


@main.command()
@click.option("--name", required=True, help="Run name to evaluate")
def evaluate(name: str):
    """Evaluate strategies from a completed run."""
    click.echo(f"Evaluating run: {name} (not yet implemented)")


@main.command()
def compare():
    """Compare results across all completed runs."""
    click.echo("Comparison not yet implemented")


@main.command()
@click.option("--name", required=True, help="Run id/path to execute (e.g., 002_ralph)")
@click.option("--max-cost-usd", type=float, default=None, help="Override max cost limit")
@click.option("--max-time-minutes", type=int, default=None, help="Override max wall time limit")
@click.option("--max-tokens-total", type=int, default=None, help="Override max total token limit")
@click.option("--max-iterations", type=int, default=None, help="Override max outer-loop iterations")
@click.option(
    "--execution-mode",
    type=click.Choice(["auto", "host", "container"]),
    default="auto",
    show_default=True,
    help="Execution mode override; auto uses architecture config.",
)
@click.option(
    "--container-runtime",
    type=click.Choice(["docker", "podman"]),
    default=None,
    help="Override container runtime when using container mode.",
)
@click.option(
    "--container-network",
    type=click.Choice(["none", "default"]),
    default=None,
    help="Override container network mode.",
)
@click.option(
    "--use-bypass-permissions/--no-use-bypass-permissions",
    default=None,
    help="Override bypassPermissions setting in container mode.",
)
def run(
    name: str,
    max_cost_usd: float | None,
    max_time_minutes: int | None,
    max_tokens_total: int | None,
    max_iterations: int | None,
    execution_mode: str,
    container_runtime: str | None,
    container_network: str | None,
    use_bypass_permissions: bool | None,
):
    """Execute a Ralph-loop run from scratch."""
    from harness.ralph_loop import RalphLoopConfig

    try:
        run_ref = resolve_run(name)
        cfg = RalphLoopConfig(
            max_cost_usd=max_cost_usd,
            max_time_minutes=max_time_minutes,
            max_tokens_total=max_tokens_total,
            max_iterations=max_iterations,
        )
        execution_overrides = _build_execution_overrides(
            execution_mode=execution_mode,
            container_runtime=container_runtime,
            container_network=container_network,
            use_bypass_permissions=use_bypass_permissions,
        )
        state = _execute_with_selected_mode(
            run_ref=run_ref,
            mode="run",
            cfg=cfg,
            execution_overrides=execution_overrides,
        )
    except Exception as e:  # noqa: BLE001 - surfacing runtime errors to CLI user.
        raise click.ClickException(str(e)) from e
    click.echo(_state_summary(state))


@main.command()
@click.option("--name", required=True, help="Run id/path to resume")
@click.option(
    "--extend-cost-usd",
    type=float,
    default=0.0,
    show_default=True,
    help="Increase max cost budget by this amount",
)
@click.option(
    "--extend-time-minutes",
    type=int,
    default=0,
    show_default=True,
    help="Increase max wall time limit by this many minutes",
)
@click.option(
    "--extend-tokens-total",
    type=int,
    default=0,
    show_default=True,
    help="Increase token limit by this amount",
)
@click.option(
    "--extend-iterations",
    type=int,
    default=0,
    show_default=True,
    help="Increase max iterations by this amount",
)
@click.option(
    "--new-session-from-checkpoint",
    is_flag=True,
    default=False,
    help="Start a fresh session seeded by on-disk checkpoints",
)
@click.option(
    "--execution-mode",
    type=click.Choice(["auto", "host", "container"]),
    default="auto",
    show_default=True,
    help="Execution mode override; auto uses architecture config.",
)
@click.option(
    "--container-runtime",
    type=click.Choice(["docker", "podman"]),
    default=None,
    help="Override container runtime when using container mode.",
)
@click.option(
    "--container-network",
    type=click.Choice(["none", "default"]),
    default=None,
    help="Override container network mode.",
)
@click.option(
    "--use-bypass-permissions/--no-use-bypass-permissions",
    default=None,
    help="Override bypassPermissions setting in container mode.",
)
def resume(
    name: str,
    extend_cost_usd: float,
    extend_time_minutes: int,
    extend_tokens_total: int,
    extend_iterations: int,
    new_session_from_checkpoint: bool,
    execution_mode: str,
    container_runtime: str | None,
    container_network: str | None,
    use_bypass_permissions: bool | None,
):
    """Resume an existing Ralph-loop run."""
    from harness.ralph_loop import RalphLoopConfig

    try:
        run_ref = resolve_run(name)
        cfg = RalphLoopConfig(
            extend_cost_usd=extend_cost_usd,
            extend_time_minutes=extend_time_minutes,
            extend_tokens_total=extend_tokens_total,
            extend_iterations=extend_iterations,
            new_session_from_checkpoint=new_session_from_checkpoint,
        )
        execution_overrides = _build_execution_overrides(
            execution_mode=execution_mode,
            container_runtime=container_runtime,
            container_network=container_network,
            use_bypass_permissions=use_bypass_permissions,
        )
        state = _execute_with_selected_mode(
            run_ref=run_ref,
            mode="resume",
            cfg=cfg,
            execution_overrides=execution_overrides,
        )
    except Exception as e:  # noqa: BLE001 - surfacing runtime errors to CLI user.
        raise click.ClickException(str(e)) from e
    click.echo(_state_summary(state))


@main.command(hidden=True)
@click.option(
    "--run-dir",
    required=True,
    type=click.Path(path_type=Path, file_okay=False, dir_okay=True, exists=True),
)
@click.option("--run-id", required=True)
@click.option("--mode", required=True, type=click.Choice(["run", "resume"]))
@click.option("--max-cost-usd", type=float, default=None)
@click.option("--max-time-minutes", type=int, default=None)
@click.option("--max-tokens-total", type=int, default=None)
@click.option("--max-iterations", type=int, default=None)
@click.option("--extend-cost-usd", type=float, default=0.0, show_default=True)
@click.option("--extend-time-minutes", type=int, default=0, show_default=True)
@click.option("--extend-tokens-total", type=int, default=0, show_default=True)
@click.option("--extend-iterations", type=int, default=0, show_default=True)
@click.option("--new-session-from-checkpoint", is_flag=True, default=False)
@click.option("--permission-mode-override", type=str, default=None)
def worker(
    run_dir: Path,
    run_id: str,
    mode: str,
    max_cost_usd: float | None,
    max_time_minutes: int | None,
    max_tokens_total: int | None,
    max_iterations: int | None,
    extend_cost_usd: float,
    extend_time_minutes: int,
    extend_tokens_total: int,
    extend_iterations: int,
    new_session_from_checkpoint: bool,
    permission_mode_override: str | None,
):
    """Internal command executed inside containerized worker."""
    from harness.ralph_loop import RalphLoopConfig, run_loop

    cfg = RalphLoopConfig(
        max_cost_usd=max_cost_usd,
        max_time_minutes=max_time_minutes,
        max_tokens_total=max_tokens_total,
        max_iterations=max_iterations,
        extend_cost_usd=extend_cost_usd,
        extend_time_minutes=extend_time_minutes,
        extend_tokens_total=extend_tokens_total,
        extend_iterations=extend_iterations,
        new_session_from_checkpoint=new_session_from_checkpoint,
        permission_mode_override=permission_mode_override,
    )
    try:
        run_loop(run_dir=run_dir, run_id=run_id, mode=mode, cfg=cfg)
    except Exception as e:  # noqa: BLE001
        raise click.ClickException(str(e)) from e
