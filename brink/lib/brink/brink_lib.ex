defmodule Brink.Lib do
  @moduledoc """
  Brink.Lib is a set of functions for building Redis commands
  """

  #@spec xread(
  #  %{required(:mode) => :single,
  #    required(:client) => String.t(),
  #    required(:stream) => String.t(),
  #    required(:next_id) => String.t(),
  #    required(:demand) => pos_integer(),
  #    optional(:initial_block_timeout) => pos_integer(),
  #    optional(any()) => any()}
  #) :: [String.t()]
  #@spec xread(
  #  %{required(:mode) => :group,
  #    required(:client) => String.t(),
  #    required(:stream) => String.t(),
  #    required(:group) => String.t(),
  #    required(:consumer) => String.t(),
  #    required(:next_id) => String.t(),
  #    required(:demand) => pos_integer(),
  #    optional(any()) => any()}
  #) :: [String.t()]
  def xread(%{stream: stream} = args) do
    case Redix.command(args.client, build_xread(args)) do
      {:ok, [[^stream, events]]} -> {:ok, events}
      {:ok, _} -> {:ok, []}
      {:error, err} -> {:error, err}
    end
  end

  @spec build_xread(%{
    required(:mode) => :single | :group,
    required(:next_id) => String.t(),
    required(:stream) => String.t(),
    required(:demand) => pos_integer(),
    optional(any()) => any()
  }) :: [String.t()]
  def build_xread(%{mode: :single, next_id: "$"} = kwargs) do
    kwargs = Map.merge(%{initial_block_timeout: 1000}, kwargs)
    [
      "XREAD",
      "BLOCK",
      kwargs.initial_block_timeout,
      "COUNT",
      kwargs.demand,
      "STREAMS",
      kwargs.stream,
      "$"
    ]
  end

  def build_xread(%{mode: :single} = kwargs) do
    [
      "XREAD",
      "COUNT",
      kwargs.demand,
      "STREAMS",
      kwargs.stream,
      kwargs.next_id
    ]
  end

  def build_xread(%{mode: :group} = kwargs) do
    [
      "XREADGROUP",
      "GROUP",
      kwargs.group,
      kwargs.consumer,
      "COUNT",
      kwargs.demand,
      "NOACK",
      "STREAMS",
      kwargs.stream,
      kwargs.next_id
    ]
  end

  #@spec format_event([String.t(), [String.t(), String.t()]]) :: {String.t(), %{key: String.t()}}
  @spec format_event(nonempty_improper_list(String.t(), [String.t()])) :: {String.t(), %{key: String.t()}}
  def format_event([id, dict]) do
    dict =
      dict
      |> Enum.chunk_every(2)
      |> Enum.map(fn [k, v] -> {:"#{k}", v} end)
      |> Map.new()

    {id, dict}
  end
end
