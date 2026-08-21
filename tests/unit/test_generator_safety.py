from ecommerce_pipeline.generator.scenarios import _reset_is_allowed


def test_source_reset_allows_only_local_hosts_by_default(monkeypatch) -> None:
    monkeypatch.delenv("ECOMMERCE_ALLOW_REMOTE_RESET", raising=False)

    assert _reset_is_allowed("localhost")
    assert _reset_is_allowed("postgres")
    assert not _reset_is_allowed("pg.example.postgres.database.azure.com")


def test_remote_source_reset_requires_explicit_override(monkeypatch) -> None:
    monkeypatch.setenv("ECOMMERCE_ALLOW_REMOTE_RESET", "1")

    assert _reset_is_allowed("pg.example.postgres.database.azure.com")
