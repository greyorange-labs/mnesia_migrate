%% migration

%% This is an autogenerate file. Please adjust
%% Fails with error(test3_fail) when the mnesia_migrate application env
%% `fail_test3` is set to true, so tests can exercise the failure path.

-module(test3_migration).
-behaviour(migration).
-export([up/0, down/0, get_current_rev/0, get_prev_rev/0, init/1]).

init([]) -> ok.

get_current_rev() ->
    test3.

get_prev_rev() ->
    test2.

up() ->
    io:format("test3: up called~n"),
    case application:get_env(mnesia_migrate, fail_test3, false) of
        true ->
            error(test3_fail);
        false ->
            ok
    end.

down() ->
   io:format("test3: down called~n").