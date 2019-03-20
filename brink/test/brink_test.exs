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

  test "handle_demand when there are no messages to read and fill demand with" do
    with_mock Brink.Consumer, [:passthrough], [read_from_stream: fn(state) -> {:noreply, [], state} end] do
      assert {:noreply, [], %{ demand: 10 }} = Brink.Consumer.handle_demand(5, %{demand: 5})
    end
  end
end
