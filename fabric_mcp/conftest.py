def pytest_addoption(parser):
    parser.addoption(
        "--keep",
        action="store_true",
        default=False,
        help="Skip cleanup: leave deployed items in the workspace after the run.",
    )
