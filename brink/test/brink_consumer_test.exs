defmodule BrinkTest do
  use ExUnit.Case
  use PropCheck
  import Mock
  doctest Brink.Consumer

  import Brink.Consumer

  test "Builds a spec in single mode" do
    {module, keywords} = Brink.Consumer.build_spec_single_mode("redis://hi", "streamname", [opt1: "hi"])
    assert module == Brink.Consumer
    assert Keyword.equal?(
      keywords,
      [mode: :single, redis_uri: "redis://hi", stream: "streamname", opt1: "hi"]
    )
  end

  property "the thing works" do
    forall {redis_uri, stream_name, kwargs} <- {utf8(), utf8(), list({atom(), term()})} do
      {module, keywords} = Brink.Consumer.build_spec_single_mode(redis_uri, stream_name, kwargs)
      assert module == Brink.Consumer
      assert Keyword.equal?(
        keywords,
        [mode: :single, redis_uri: redis_uri, stream: stream_name] ++ kwargs
      )
    end
  end

  defp msgs(num) do
    List.duplicate(["#{num}", ["thekey", "#{num}"]], num)
  end

  test "handle_demand buffers demand when there is nothing to send back" do
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

  test "buffer leftover demand when there aren't enough messages to satisfy it" do
    with_mock Brink.Lib, [:passthrough], [xread: fn(_) -> {:ok, msgs(3)} end] do
      opts = %{mode: :single, next_id: "$", poll_interval: 100}
      {:noreply, _, %{demand: 2}} = handle_demand(5, Map.put(opts, :demand, 0))
      {:noreply, _, %{demand: 1}}  = handle_demand(2,  Map.put(opts, :demand,  2))
      {:noreply, _, %{demand: 15}}  = handle_demand(12,  Map.put(opts, :demand,  6))
    end
  end

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
end
