defmodule BrinkTest do
  use ExUnit.Case
  use PropCheck
  import Mock
  doctest Brink.Consumer

  import Brink.Consumer

  @tag :skip
  test "Builds a spec in single mode" do
    {module, keywords} =
      Brink.Consumer.build_spec_single_mode("redis://hi", "streamname", opt1: "hi")

    assert module == Brink.Consumer

    assert Keyword.equal?(
             keywords,
             mode: :single,
             redis_uri: "redis://hi",
             stream: "streamname",
             opt1: "hi"
           )
  end

  @tag :skip
  test "handle_demand buffers demand when there is nothing to send back" do
    events = []
    prev_demand = 5
    incoming_demand = 4

    xread = fn %{demand: demand} ->
      # Verify demand is passed down to Brink.Lib.xread properly
      assert demand == 9
      {:ok, events}
    end

    with_mock Brink.Lib, [:passthrough], xread: xread do
      assert(
        {:noreply, events, %{demand: 9, mode: :single, next_id: "$", poll_interval: 100}} ==
          Brink.Consumer.handle_demand(incoming_demand, %{
            mode: :single,
            demand: prev_demand,
            next_id: "$",
            poll_interval: 100
          })
      )
    end
  end

  # A list of adjacent elements [W, X, Y, Z] where W is the key of value X and
  # Y is the key of value Z
  def dictionary() do
    let m <- map(utf8(), utf8()) do
      Enum.reduce(m, [], fn {k, v}, acc -> [k | [v | acc]] end)
    end
  end

  def to_range(m, n) do
    base = div(n, m)
    {base * m, (base + 1) * m}
  end

  defp msgs(num) do
    List.duplicate(["#{num}", ["thekey", "#{num}"]], num)
  end

  def timestamp_v1() do
    let ts <- resize(1000000, integer(0, :inf)) do
      ts
    end
  end

  property "collect timestamps", [:verbose] do
    forall x <- timestamp_v1() do
      collect(is_integer(x), to_range(1000000, x))
    end
  end

  @tag :skip
  test "handle_demand buffers demand when there is nothing to send back2" do
    with_mock Brink.Lib, [:passthrough], [xread: fn(_) -> {:ok, msgs(0)} end] do
      opts = %{mode: :single, next_id: "$", poll_interval: 100}
      {:noreply, _, %{demand: 45}} = handle_demand(25, Map.put(opts, :demand, 20))
      {:noreply, _, %{demand: 9}}  = handle_demand(4,  Map.put(opts, :demand,  5))
      {:noreply, _, %{demand: 5}}  = handle_demand(0,  Map.put(opts, :demand,  5))
      {:noreply, _, %{demand: 4}}  = handle_demand(4,  Map.put(opts, :demand,  0))
      {:noreply, _, %{demand: 0}}  = handle_demand(0,  Map.put(opts, :demand,  0))
      # TODO can demand be negative??
    end
  end

  @tag :skip
  test "buffer leftover demand when there aren't enough messages to satisfy it" do
    with_mock Brink.Lib, [:passthrough], [xread: fn(_) -> {:ok, msgs(3)} end] do
      opts = %{mode: :single, next_id: "$", poll_interval: 100}
      {:noreply, _, %{demand: 2}} = handle_demand(5, Map.put(opts, :demand, 0))
      {:noreply, _, %{demand: 1}}  = handle_demand(2,  Map.put(opts, :demand,  2))
      {:noreply, _, %{demand: 15}}  = handle_demand(12,  Map.put(opts, :demand,  6))
    end
  end

  @tag :skip
  test "satisfy demand when there are enough messages" do
    with_mock Brink.Lib, [:passthrough], [xread: fn(_) -> {:ok, msgs(10)} end] do
      opts = %{mode: :single, next_id: "$", poll_interval: 100}
      {:noreply, _, %{demand: 0}} = handle_demand(8, Map.put(opts, :demand, 2))
      # TODO figure out if negative demand is a thing
      #{:noreply, _, %{demand: 0}}  = handle_demand(1,  Map.put(opts, :demand,  2))
      #{:noreply, _, %{demand: 0}}  = handle_demand(1,  Map.put(opts, :demand,  0))
      {:noreply, _, %{demand: 0}}  = handle_demand(0,  Map.put(opts, :demand,  0))
    end
  end

  # NOTE: Brink.Lib.xread will never return more events than are demanded, since
  #       xread uses total demand as the COUNT parameter in the Redis call
  @tag :skip
  test "handle_demand has enough messages to fulfill total demand" do
    events = msgs(9)
    prev_demand = 5
    incoming_demand = 4

    xread = fn %{demand: demand} ->
      # Verify demand is passed down to Brink.Lib.xread properly
      assert demand == 9
      {:ok, events}
    end

    with_mock Brink.Lib, [:passthrough], xread: xread do
      assert(
        {:noreply, events, %{demand: 0, mode: :single, next_id: "1234-9", poll_interval: 100}} ==
          Brink.Consumer.handle_demand(incoming_demand, %{
            mode: :single,
            demand: prev_demand,
            next_id: "$",
            poll_interval: 100
          })
      )
    end
  end
end
