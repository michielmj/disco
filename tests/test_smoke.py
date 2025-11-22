def test_import():
    import disco
    from disco import __version__
    assert isinstance(__version__, str)

def test_core_example():
    from disco import _core
    assert _core.example() == 42

