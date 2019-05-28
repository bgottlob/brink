defmodule BrinkTest do
  use ExUnit.Case
  use PropCheck
  import Mock
  doctest Brink.Consumer

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

  test "handle_demand buffers remaining demand when there is not enough to send back" do
    events = gen_events(1)
    prev_demand = 5
    incoming_demand = 4

    xread = fn %{demand: demand} ->
      # Verify demand is passed down to Brink.Lib.xread properly
      assert demand == 9
      {:ok, events}
    end

    with_mock Brink.Lib, [:passthrough], xread: xread do
      assert(
        {:noreply, events, %{demand: 8, mode: :single, next_id: "1234-1", poll_interval: 100}} ==
          Brink.Consumer.handle_demand(prev_demand, %{
            mode: :single,
            demand: incoming_demand,
            next_id: "$",
            poll_interval: 100
          })
      )
    end
  end

  # NOTE: Brink.Lib.xread will never return more events than are demanded, since
  #       xread uses total demand as the COUNT parameter in the Redis call
  test "handle_demand has enough messages to fulfill total demand" do
    events = gen_events(9)
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

  defp gen_events(n) do
    Enum.map(1..n, fn x -> {"1234-#{n}", %{:"key_#{x}" => "value_#{x}"}} end)
  end
end
