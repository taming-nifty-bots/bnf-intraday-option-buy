"""Offline BNF execution-boundary checks; no broker or Mongo connection."""
import ast
import datetime
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tamingnifty"))
from tamingnifty.safety import assert_execution_clear, guarded_execution
from pymongo.errors import DuplicateKeyError

SOURCE = Path(__file__).parent / "bnf-option-buy" / "option_buy.py"


def load_functions(names, namespace):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8-sig"))
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(nodes) == len(names)
    assert all(not n.decorator_list for n in nodes)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), "exec"), namespace)


def guard_client():
    documents = {}

    def update(query, changes):
        document = documents.get(query["_id"])
        matched = document is not None and all(document.get(k) == v for k, v in query.items())
        if matched:
            document.update(changes["$set"])
        return SimpleNamespace(acknowledged=True, matched_count=int(matched))

    def insert(document):
        if document["_id"] in documents:
            raise DuplicateKeyError("existing scope")
        documents[document["_id"]] = dict(document)
        return SimpleNamespace(acknowledged=True)

    collection = SimpleNamespace(update_one=update, insert_one=insert,
                                 find_one=lambda query: documents.get(query["_id"]))
    return {"Bots": {"execution_guards": collection}}, documents


def assert_stopped(function, *args):
    try:
        function(*args)
    except SystemExit as error:
        assert error.code == 1
        return
    raise AssertionError("Expected fail-stop")


def test_half_book_failure_after_order_blocks_polling_and_restart():
    client, documents = guard_client()
    broker = Mock(return_value=dict(average_traded_price=110))
    namespace = dict(mongo_client=client, EXECUTION_SCOPE="Bots.banknifty_test",
                     notify=Mock(), guarded_execution=guarded_execution,
                     place_sell_order=broker, half_quantity=30, datetime=datetime,
                     strategies=SimpleNamespace(update_one=Mock(
                         side_effect=RuntimeError("state write failed"))))
    load_functions({"run_execution", "book_half", "calculate_pnl", "check_write"}, namespace)
    strategy = dict(_id="trade", quantity=60, long_option_symbol="TEST", long_option_cost=100)
    args = ("half book", namespace["book_half"], strategy)
    assert_stopped(namespace["run_execution"], *args)
    assert broker.call_count == 1
    assert documents[namespace["EXECUTION_SCOPE"]]["status"] == "failed"
    try:
        assert_execution_clear(client, namespace["EXECUTION_SCOPE"], namespace["notify"])
    except RuntimeError:
        pass
    else:
        raise AssertionError("Restart must remain blocked")
    assert_stopped(namespace["run_execution"], *args)
    assert broker.call_count == 1


def test_unacknowledged_strategy_write_cannot_complete_guard():
    client, documents = guard_client()
    namespace = dict(mongo_client=client, EXECUTION_SCOPE="Bots.banknifty_test",
                     notify=Mock(), guarded_execution=guarded_execution)
    load_functions({"run_execution", "check_write"}, namespace)
    persisted = SimpleNamespace(acknowledged=False)
    assert_stopped(namespace["run_execution"], "entry", namespace["check_write"], persisted, "entry")
    assert documents[namespace["EXECUTION_SCOPE"]]["status"] == "failed"


def test_successful_half_book_persists_before_guard_completion():
    client, documents = guard_client()
    scope = "Bots.banknifty_test"
    broker = Mock(return_value=dict(average_traded_price=110))
    def update(query, changes):
        assert broker.call_count == 1
        assert documents[scope]["status"] == "pending"
        assert changes["$set"]["strategy_state"] == "partial"
        assert changes["$set"]["runner_quantity"] == 30
        return SimpleNamespace(acknowledged=True, matched_count=1)
    namespace = dict(mongo_client=client, EXECUTION_SCOPE=scope, notify=Mock(),
                     guarded_execution=guarded_execution, place_sell_order=broker,
                     half_quantity=30, datetime=datetime,
                     strategies=SimpleNamespace(update_one=update),
                     util=SimpleNamespace(notify=Mock()), slack_client=Mock(), slack_channel="test")
    load_functions({"run_execution", "book_half", "calculate_pnl", "check_write"}, namespace)
    strategy = dict(_id="trade", quantity=60, long_option_symbol="TEST", long_option_cost=100)
    namespace["run_execution"]("half book", namespace["book_half"], strategy)
    assert documents[scope]["status"] == "complete"
    assert_execution_clear(client, scope, namespace["notify"])


def test_all_main_execution_paths_are_guarded_and_have_no_retry_decorators():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8-sig"))
    protected = {"place_buy_order", "place_sell_order", "buy_call", "buy_put",
                 "record_details_in_mongo", "close_active_positions", "book_half"}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in protected:
            assert not node.decorator_list, node.name
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main")
    assert ast.unparse(main.body[1].value.func) == "assert_execution_clear"
    for node in ast.walk(main):
        if isinstance(node, ast.Call):
            assert ast.unparse(node.func) not in protected
    wrapped = [n for n in ast.walk(main) if isinstance(n, ast.Call)
               and isinstance(n.func, ast.Name) and n.func.id == "run_execution"]
    assert len(wrapped) == 8
    assert {ast.unparse(n.args[1]) for n in wrapped} == {
        "buy_call", "buy_put", "close_active_positions", "book_half"}


if __name__ == "__main__":
    tests = [value for key, value in sorted(globals().items()) if key.startswith("test_")]
    for test in tests:
        test()
        print("PASS", test.__name__)
    print(len(tests), "passed")
