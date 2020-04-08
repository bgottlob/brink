defmodule BrinkTest.Lib do
  use ExUnit.Case
  doctest Brink.Lib

  @tag :skip
  test "the default initial block timeout is set" do
    command =
      Brink.Lib.build_xread(%{
        mode: :single,
        demand: 10,
        stream: "test_stream",
        next_id: "$"
      })

    assert Enum.at(command, 2) == 1000
  end

  @tag :skip
  test "demand is passed to Redis command properly for each function clause of build_xread" do
    assert(
      ["XREAD", "BLOCK", 100, "COUNT", 10, "STREAMS", "test_stream", "$"] ==
        Brink.Lib.build_xread(%{
          mode: :single,
          initial_block_timeout: 100,
          demand: 10,
          stream: "test_stream",
          next_id: "$"
        })
    )

    assert(
      ["XREAD", "COUNT", 15, "STREAMS", "test_stream", "1234-1"] ==
        Brink.Lib.build_xread(%{
          mode: :single,
          demand: 15,
          stream: "test_stream",
          next_id: "1234-1"
        })
    )

    assert(
      [
        "XREADGROUP",
        "GROUP",
        "test_group",
        "test_consumer",
        "COUNT",
        5,
        "NOACK",
        "STREAMS",
        "test_stream",
        "1234-1"
      ] ==
        Brink.Lib.build_xread(%{
          mode: :group,
          group: "test_group",
          consumer: "test_consumer",
          demand: 5,
          stream: "test_stream",
          next_id: "1234-1"
        })
    )
  end

  @tag :skip
  test "Formats key-value dictionaries from events coming out of Redis" do
    assert(
      {"timestamp-0", %{one: "a", two: "b", three: "c"}} ==
        Brink.Lib.format_event(["timestamp-0", ["one", "a", "two", "b", "three", "c"]])
    )
  end

  @tag :skip
  test "Edge cases for format_event" do
    # No data in event
    assert({"timestamp-2", %{}} == Brink.Lib.format_event(["timestamp-2", []]))
    # Duplicate keys - takes the last value for the key
    assert(
      {"timestamp-3", %{one: "d", two: "c"}} ==
        Brink.Lib.format_event(["timestamp-3", ["one", "a", "one", "b", "two", "c", "one", "d"]])
    )
  end
end
