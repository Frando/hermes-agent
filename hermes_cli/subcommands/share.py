"""``hermes share`` and ``hermes join`` subcommand parsers.

``share`` launches the TUI with sharing enabled; ``join`` attaches the TUI to a
shared session over iroh. Handlers are injected to avoid importing ``main``.
"""

from __future__ import annotations

from typing import Callable


def build_share_parser(subparsers, *, cmd_share: Callable, cmd_join: Callable) -> None:
    """Attach the ``share`` and ``join`` subcommands to ``subparsers``."""
    share_parser = subparsers.add_parser(
        "share",
        help="Launch the TUI and share this session over iroh",
        description=(
            "Launch the Hermes TUI with sharing enabled. Prints a watch ticket "
            "and a control ticket; others run `hermes join <ticket>` to follow "
            "along or, with the control ticket, drive the agent."
        ),
    )
    share_parser.add_argument(
        "--dev", action="store_true", help="Run the TUI from TypeScript sources via tsx"
    )
    share_parser.set_defaults(func=cmd_share)

    join_parser = subparsers.add_parser(
        "join",
        help="Join a shared session via its ticket",
        description=(
            "Attach the Hermes TUI to a session shared with `hermes share`. A "
            "watch ticket follows along; a control ticket can take control with "
            "/grab and drive the agent."
        ),
    )
    join_parser.add_argument("ticket", help="The watch or control ticket to join")
    join_parser.add_argument(
        "--name", default="guest", help="Name shown to other participants"
    )
    join_parser.add_argument(
        "--dev", action="store_true", help="Run the TUI from TypeScript sources via tsx"
    )
    join_parser.set_defaults(func=cmd_join)
