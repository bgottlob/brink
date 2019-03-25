defmodule BrinkTest do
  use ExUnit.Case
  use PropCheck
  import Mock
  doctest Brink.Consumer

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

  test "handle_demand buffers demand when there is nothing to send back" do
    with_mock Brink.Lib, [:passthrough], [xread: fn(_) -> {:ok, [["123-1", ["hey", "joe"]]]} end] do
      assert {:noreply, [{"123-1", %{hey: "joe"}}], %{ demand: 9, mode: :single, next_id: "123-1", poll_interval: 100 }} == Brink.Consumer.handle_demand(5, %{mode: :single, demand: 5, next_id: "$", poll_interval: 100})
    end
  end

  #property "handle_demand buffers demand when there is nothing to send back" do
  #  forall {event} <- {event()} do
  #    with_mock Brink.Lib, [:passthrough], [xread: fn(_) -> {:ok, [event]} end] do
  #      assert {:noreply, [event], %{ demand: 9 }} == Brink.Consumer.handle_demand(5, %{mode: :single, demand: 5, next_id: "$", poll_interval: 100})
  #    end
  #  end
  #end

  def event(), do: [utf8(), [utf8(), utf8()]]
end
