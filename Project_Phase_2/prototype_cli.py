from __future__ import annotations

from prototype.orchestrator import AgenticOrchestrator


def main() -> None:
    orchestrator = AgenticOrchestrator()
    print("Prototype router ready. Type 'exit' to stop.")

    while True:
        q = input("\nYou: ").strip()
        if q.lower() in {"exit", "quit"}:
            break
        out = orchestrator.invoke(q)
        print("\nAssistant:")
        print(out["response"])
        print(f"\n[routed domain={out['active_domain']} story={out['active_story_id']}]")


if __name__ == "__main__":
    main()
