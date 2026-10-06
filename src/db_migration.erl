%% @author Gaurav Kumar <gauravkumar552@gmail.com>

-module(db_migration).
-compile(export_all).
-compile(nowarn_export_all).

-define(TABLE, schema_migrations).
-define(RUN_TABLE, db_migration_runs).

-record(schema_migrations, {prime_key = null, curr_head = null}).

-record(db_migration_runs, {
    id :: {Tag :: any(), MigrationName :: atom(), AttemptTs :: integer()},
    tag :: any(),
    migration_name :: atom(),
    direction :: up | down,
    status :: running | ok | failed,
    started_at :: calendar:local_time(),
    finished_at :: calendar:local_time() | undefined,
    error_reason :: {Class :: atom(), Reason :: any()} | undefined,
    stacktrace :: binary() | undefined,
    node :: node()
}).

read_config() ->
    Val = application:get_env(mnesia_migrate, migration_dir, "~/project/mnesia_migrate/src/migrations/"),
    print("migration_dir: ~p", [Val]).

start_mnesia() ->
    mnesia:start().

init_migrations() ->
    case lists:member(?TABLE, mnesia:system_info(tables)) of
        true ->
            ok;
        false ->
            Attr = [{disc_copies, [node()]}, {attributes, record_info(fields, schema_migrations)}],
            case mnesia:create_table(?TABLE, Attr) of
                {atomic, ok} ->
                    ok;
                {aborted, Reason} ->
                    throw({error, Reason})
            end
    end,
    case lists:member(?RUN_TABLE, mnesia:system_info(tables)) of
        true ->
            ok;
        false ->
            RunAttr = [
                {disc_copies, [node()]},
                {attributes, record_info(fields, db_migration_runs)}
            ],
            case mnesia:create_table(?RUN_TABLE, RunAttr) of
                {atomic, ok} ->
                    ok;
                {aborted, RunReason} ->
                    throw({error, RunReason})
            end
    end,
    TimeOut = application:get_env(mnesia_migrate, table_load_timeout, 10000),
    ok = mnesia:wait_for_tables([?TABLE, ?RUN_TABLE], TimeOut).

get_current_time() ->
    calendar:local_time().

-spec run_migrations() -> ok.
run_migrations() ->
    ok = init_migrations(),
    print("~p: Applying migrations.........", [?MODULE]),
    case get_dangling_migrations() of
        [] ->
            CurrentHead = get_current_head(),
            CurrentAppliedHead = get_applied_head(),
            print("Current head = ~p, Current applied head = ~p", [CurrentHead, CurrentAppliedHead]),
            PendingMigrations = find_pending_migrations(),
            print("Migrations to apply: ~p", [PendingMigrations]),
            case PendingMigrations of
                [] ->
                    ok;
                _PendingMigrations ->
                    {ok, applied} = apply_upgrades(PendingMigrations)
            end;
        DanglingMigrations ->
            print("~Error!!! ~p: Dangling migrations found: ~p", [?MODULE, DanglingMigrations]),
            exit("Dangling migrations found")
    end,
    ok.

%%
%%Functions related to migration info
%%

get_current_head() ->
    BaseRev = get_base_revision(),
    get_current_head(BaseRev).

get_revision_tree() ->
    BaseRev = get_base_revision(),
    List1 = [],
    RevList = append_revision_tree(List1, BaseRev),
    RevList.

get_down_revision_tree() ->
    BaseRev = get_applied_head(),
    List1 = [],
    RevList = append_down_revision_tree(List1, BaseRev),
    RevList.

find_pending_migrations() ->
    RevList =
        case get_applied_head() of
            none ->
                case get_base_revision() of
                    none -> [];
                    BaseRevId -> append_revision_tree([], BaseRevId)
                end;
            Id ->
                case get_next_revision(Id) of
                    [] -> [];
                    NextId -> append_revision_tree([], NextId)
                end
        end,
    RevList.

%%
%% Functions related to migration creation
%%

create_migration_file(_CommitMessage) ->
    io:format(
        "----------------------------------------------------------------------------------------------------------------------~n"
    ),
    io:format(
        "----------------------------------------------------------------------------------------------------------------------~n"
    ),
    io:format("  ~s~n~n~n", [
        color:p(
            "NOTE: Using `mnesia_migrate` to create migrations is no longer allowed. Please use the following method instead:- ",
            [bold, red]
        )
    ]),
    io:format("  ~s~n", [color:p("- For GMC Application: ", [bold, green])]),
    io:format("    ~s~n~n", [color:p("gmc_db_setup:create_migration_file().", [bold, blue])]),
    io:format("  ~s~n", [color:p("- For GMR Applications (pick, put, audit, station, etc): ", [bold, green])]),
    io:format("    ~s~n~n", [color:p("gmr_db_setup:create_migration_file().", [bold, blue])]),
    io:format("  ~s~n", [color:p("- For Butler Base/Shared Application: ", [bold, green])]),
    io:format("    ~s~n~n", [color:p("gm_base_db_setup:create_migration_file().", [bold, blue])]),
    io:format("  ~s~n", [color:p("The above methods uses https://github.com/greyorange-labs/erl_migrate ", [bold, yellow])]),
    io:format(
        "----------------------------------------------------------------------------------------------------------------------~n"
    ),
    io:format(
        "----------------------------------------------------------------------------------------------------------------------~n"
    ).

create_migration_file() ->
    create_migration_file("None").

%%
%% Functions related to applying migrations
%%

apply_upgrades(PendingMigrations) ->
    case PendingMigrations of
        [] ->
            ok;
        _ ->
            lists:foreach(
                fun(RevId) ->
                    ModuleName = list_to_atom(atom_to_list(RevId) ++ "_migration"),
                    print("Applying migration: ~p", [RevId]),
                    ok = run_revision(up, RevId, ModuleName),
                    update_head(RevId)
                end,
                PendingMigrations
            ),
            print("~p: All pending migration successfully applied.", [?MODULE])
    end,
    {ok, applied}.

apply_downgrades(DownNum) ->
    CurrHead = get_applied_head(),
    Count = get_count_between_2_revisions(get_base_revision(), CurrHead),
    case DownNum =< Count of
        false ->
            print("Wrong number for downgrade", []),
            {error, wrong_number};
        true ->
            RevList = get_down_revision_tree(),
            SubList = lists:sublist(RevList, 1, DownNum),
            case SubList of
                [] ->
                    print("No down revision found", []);
                _ ->
                    lists:foreach(
                        fun(RevId) ->
                            ModuleName = list_to_atom(atom_to_list(RevId) ++ "_migration"),
                            print("Running downgrade ~p -> ~p", [ModuleName:get_current_rev(), ModuleName:get_prev_rev()]),
                            ok = run_revision(down, RevId, ModuleName),
                            update_head(ModuleName:get_prev_rev())
                        end,
                        SubList
                    ),
                    print("all downgrades successfully applied.", [])
            end
    end.

%%
%% Functions related to run observability
%%

-spec run_revision(
    Direction :: up | down,
    RevId :: atom(),
    ModuleName :: module()
) -> ok.
run_revision(Direction, RevId, ModuleName) ->
    Args = #{schema_name => legacy, schema_instance => legacy},
    AttemptTs = erlang:system_time(microsecond),
    StartedAt = get_current_time(),
    ok = write_run_log(RevId, Args, Direction, running, AttemptTs, StartedAt, undefined, undefined, undefined),
    notify_observer(on_revision_start, Args, [RevId]),
    StartMs = erlang:monotonic_time(millisecond),
    try
        ModuleName:up(),
        FinishMs = erlang:monotonic_time(millisecond) - StartMs,
        ok = write_run_log(RevId, Args, Direction, ok, AttemptTs, StartedAt, get_current_time(), undefined, undefined),
        notify_observer(on_revision_ok, Args, [RevId, FinishMs]),
        ok
    catch
        Class:Reason:Stack ->
            FailMs = erlang:monotonic_time(millisecond) - StartMs,
            StackTrace = format_stacktrace(Stack),
            ok = write_run_log(
                RevId, Args, Direction, failed, AttemptTs, StartedAt, get_current_time(), {Class, Reason}, StackTrace
            ),
            notify_observer(on_revision_failed, Args, [RevId, FailMs, {Class, Reason, Stack}]),
            erlang:raise(Class, Reason, Stack)
    end.

-spec write_run_log(
    RevId :: atom(),
    Args :: maps:map(),
    Direction :: up | down,
    Status :: running | ok | failed,
    AttemptTs :: integer(),
    StartedAt :: calendar:local_time(),
    FinishedAt :: calendar:local_time() | undefined,
    ErrorReason :: {Class :: atom(), Reason :: any()} | undefined,
    StackTrace :: binary() | undefined
) -> ok.
write_run_log(RevId, _Args, Direction, Status, AttemptTs, StartedAt, FinishedAt, ErrorReason, StackTrace) ->
    Tag = application:get_env(mnesia_migrate, run_tag, legacy),
    Id = {Tag, RevId, AttemptTs},
    Rec = #db_migration_runs{
        id = Id,
        tag = Tag,
        migration_name = RevId,
        direction = Direction,
        status = Status,
        started_at = StartedAt,
        finished_at = FinishedAt,
        error_reason = ErrorReason,
        stacktrace = StackTrace,
        node = node()
    },
    {atomic, ok} = mnesia:transaction(fun() -> mnesia:write(?RUN_TABLE, Rec, write) end),
    ok.

-spec notify_observer(Callback :: atom(), Args :: maps:map(), Payload :: list()) -> ok.
notify_observer(Callback, Args, Payload) ->
    Schema = maps:get(schema_name, Args, undefined),
    Instance = maps:get(schema_instance, Args, undefined),
    case application:get_env(mnesia_migrate, run_log_observer, undefined) of
        undefined ->
            ok;
        Observer ->
            CallArgs = [Schema, Instance | Payload] ++ [Args],
            _ = try apply(Observer, Callback, CallArgs)
                catch _:_ -> ok
            end,
            ok
    end.

-spec format_stacktrace(Stack :: list()) -> binary().
format_stacktrace(Stack) ->
    iolist_to_binary(io_lib:format("~p", [Stack])).

-spec get_last_migration_run() -> #db_migration_runs{} | none.
get_last_migration_run() ->
    Rows = mnesia:dirty_match_object(?RUN_TABLE, #db_migration_runs{tag = run_tag(), _ = '_'}),
    case Rows of
        [] ->
            none;
        _ ->
            lists:foldl(
                fun(Run, Last) ->
                    case attempt_ts(Run) > attempt_ts(Last) of
                        true -> Run;
                        false -> Last
                    end
                end,
                hd(Rows),
                tl(Rows)
            )
    end.

-spec get_run_log() -> list(#db_migration_runs{}).
get_run_log() ->
    mnesia:dirty_match_object(?RUN_TABLE, #db_migration_runs{tag = run_tag(), _ = '_'}).

run_tag() ->
    application:get_env(mnesia_migrate, run_tag, legacy).

attempt_ts(#db_migration_runs{id = {_Tag, _RevId, AttemptTs}}) ->
    AttemptTs.

append_revision_tree(List1, RevId) ->
    case get_next_revision(RevId) of
        [] ->
            List1 ++ [RevId];
        NewRevId ->
            List2 = List1 ++ [RevId],
            append_revision_tree(List2, NewRevId)
    end.

append_down_revision_tree(List1, RevId) ->
    case get_prev_revision(RevId) of
        [] ->
            List1 ++ [RevId];
        NewRevId ->
            List2 = List1 ++ [RevId],
            append_down_revision_tree(List2, NewRevId)
    end.

get_applied_head() ->
    {atomic, KeyList} = mnesia:transaction(fun() -> mnesia:read(schema_migrations, head) end),
    Head =
        case length(KeyList) of
            0 ->
                none;
            _ ->
                Rec = hd(KeyList),
                Rec#schema_migrations.curr_head
        end,
    Head.

update_head(Head) ->
    mnesia:transaction(fun() ->
        case mnesia:wread({schema_migrations, head}) of
            [] ->
                mnesia:write(schema_migrations, #schema_migrations{prime_key = head, curr_head = Head}, write);
            [CurrRec] ->
                mnesia:write(CurrRec#schema_migrations{curr_head = Head})
        end
    end).

%%
%% Post migration validations
%%

-spec detect_conflicts_post_migration([{tuple(), list()}]) -> list().
detect_conflicts_post_migration(Models) ->
    ConflictingTables = [
        TableName
     || {TableName, Options} <- Models, proplists:get_value(attributes, Options) /= mnesia:table_info(TableName, attributes)
    ],
    print("~p: Tables having conflicts in structure after applying migrations: ~p", [?MODULE, ConflictingTables]),
    ConflictingTables.

%%
%% helper functions
%%

has_migration_behaviour(Modulename) ->
    case catch Modulename:module_info(attributes) of
        {'EXIT', {undef, _}} ->
            false;
        Attributes ->
            case lists:keyfind(behaviour, 1, Attributes) of
                {behaviour, BehaviourList} ->
                    lists:member(migration, BehaviourList);
                false ->
                    false
            end
    end.

get_base_revision() ->
    Modulelist = filelib:wildcard(get_migration_beam_filepath() ++ "*_migration.beam"),
    Res = lists:filter(
        fun(Filename) ->
            Modulename = list_to_atom(filename:basename(Filename, ".beam")),
            case has_migration_behaviour(Modulename) of
                true -> Modulename:get_prev_rev() =:= none;
                false -> false
            end
        end,
        Modulelist
    ),
    BaseModuleName = list_to_atom(filename:basename(Res, ".beam")),
    case Res of
        [] -> none;
        _ -> BaseModuleName:get_current_rev()
    end.

get_prev_revision(RevId) ->
    CurrModuleName = list_to_atom(atom_to_list(RevId) ++ "_migration"),
    Modulelist = filelib:wildcard(get_migration_beam_filepath() ++ "*_migration.beam"),
    Res = lists:filter(
        fun(Filename) ->
            Modulename = list_to_atom(filename:basename(Filename, ".beam")),
            case has_migration_behaviour(Modulename) of
                true -> Modulename:get_current_rev() =:= CurrModuleName:get_prev_rev();
                false -> false
            end
        end,
        Modulelist
    ),
    case Res of
        [] ->
            [];
        _ ->
            ModuleName = list_to_atom(filename:basename(Res, ".beam")),
            ModuleName:get_current_rev()
    end.

get_next_revision(RevId) ->
    Modulelist = filelib:wildcard(get_migration_beam_filepath() ++ "*_migration.beam"),
    Res = lists:filter(
        fun(Filename) ->
            Modulename = list_to_atom(filename:basename(Filename, ".beam")),
            case has_migration_behaviour(Modulename) of
                true -> Modulename:get_prev_rev() =:= RevId;
                false -> false
            end
        end,
        Modulelist
    ),
    case Res of
        [] ->
            [];
        _ ->
            ModuleName = list_to_atom(filename:basename(Res, ".beam")),
            ModuleName:get_current_rev()
    end.

get_current_head(RevId) ->
    case get_next_revision(RevId) of
        [] -> RevId;
        NextRevId -> get_current_head(NextRevId)
    end.

get_migration_source_filepath() ->
    Val = application:get_env(mnesia_migrate, migration_source_dir, "src/migrations/"),
    ok = filelib:ensure_dir(Val),
    Val.

get_migration_beam_filepath() ->
    Val = application:get_env(mnesia_migrate, migration_beam_dir, "ebin/"),
    ok = filelib:ensure_dir(Val),
    Val.

get_count_between_2_revisions(RevStart, RevEnd) ->
    RevList = get_revision_tree(),
    Count = string:str(RevList, [RevEnd]) - string:str(RevList, [RevStart]),
    Count.

print(Statement) ->
    print(Statement, []).

print(Statement, Arg) ->
    case application:get_env(mnesia_migrate, verbose, true) of
        true -> io:format(Statement ++ "~n", Arg);
        false -> ok
    end.

%% Post migration validations
-spec get_dangling_migrations() -> DanglingMigrationList :: list(atom()).
get_dangling_migrations() ->
    RevisionTreeMigrationList = db_migration:get_revision_tree(),
    MigrationFiles = filelib:wildcard(
        application:get_env(mnesia_migrate, migration_beam_dir, "ebin/") ++ "*_migration.beam"
    ),
    MigrationList = [
        list_to_atom(filename:basename(MigrationFile, "_migration.beam"))
     || MigrationFile <- MigrationFiles
    ],
    MigrationList -- RevisionTreeMigrationList.

-spec detect_revision_sequence_conflicts() -> list().
detect_revision_sequence_conflicts() ->
    Tree = get_revision_tree(),
    Modulelist = filelib:wildcard(get_migration_beam_filepath() ++ "*_migration.beam"),
    ConflictId = lists:filter(
        fun(RevId) ->
            Res = lists:filter(
                fun(Filename) ->
                    Modulename = list_to_atom(filename:basename(Filename, ".beam")),
                    case has_migration_behaviour(Modulename) of
                        true -> Modulename:get_prev_rev() =:= RevId;
                        false -> false
                    end
                end,
                Modulelist
            ),
            case length(Res) > 1 of
                true ->
                    print("Conflict detected at revision id ~p", [RevId]),
                    true;
                false ->
                    false
            end
        end,
        Tree
    ),
    ConflictId.
