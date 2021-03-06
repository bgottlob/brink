defmodule BrinkDemo.Consumer do
  use Flow

  def start_link(options) do
    redis_uri = Keyword.fetch!(options, :redis_uri)
    stream = Keyword.fetch!(options, :stream)
    group = Keyword.fetch!(options, :group)
    consumer = Keyword.fetch!(options, :consumer)

    Flow.from_specs(
      [
        Brink.Consumer.build_spec_group_mode(
          redis_uri,
          stream,
          group,
          name: :"Brink.Producer-#{consumer}",
          consumer: consumer
        )
      ],
      window: Flow.Window.periodic(3, :second)
    )
    |> Flow.reduce(fn -> {0, 0, ""} end, fn {_id, %{now: now, value: value}},
                                            {count, total_time, _time} ->
      {count + 1, total_time + elem(Integer.parse(value), 0), now}
    end)
    |> Flow.on_trigger(fn {count, total_time, time}, partition ->
      IO.inspect({:consumer, consumer, partition, count, div(total_time, count), time})
      {[], {0, 0, ""}}
    end)
    |> Flow.start_link()
  end
end
