  #########
  # BUILD #
  #########
FROM beardedeagle/alpine-elixir-builder:1.7 as build
COPY config ./config
COPY rel ./rel
COPY mix.exs .
COPY mix.lock .
RUN export MIX_ENV=prod && \
  mix deps.get && \
  mix deps.compile
COPY lib ./lib
RUN export MIX_ENV=prod && \
  mix release
RUN RELEASE_DIR=`ls -d _build/prod/rel/brink_demo/releases/*/` && \
  mkdir /export && \
  tar -xf "$RELEASE_DIR/brink_demo.tar.gz" -C /export

  ##########
  # DEPLOY #
  ##########
FROM bitwalker/alpine-elixir:1.7
COPY --from=build /export/ .
USER default
ENTRYPOINT ["/opt/app/bin/brink_demo"]
CMD ["foreground"]
