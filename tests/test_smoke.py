def test_skeleton_imports():
    """骨架冒烟：所有模块可导入。"""
    import app.normalizer  # noqa: F401
    import app.storage  # noqa: F401
    import app.buffer  # noqa: F401
    import app.uplink  # noqa: F401
    import app.detector  # noqa: F401
    import app.gateway  # noqa: F401
    import main  # noqa: F401
