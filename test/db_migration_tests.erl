-module(db_migration_tests).

-include_lib("eunit/include/eunit.hrl").

check_file_creation_test_() ->
	[
	 {"Check Src Dir Exists",
	   ?_assertEqual(true, filelib:is_dir(db_migration:get_migration_source_filepath()))},
	 {"Check Beam Dir Exists",
	   ?_assertEqual(true, filelib:is_dir(db_migration:get_migration_beam_filepath()))}
	].

check_function_output_format_test_() ->
	[
	 {"check_get_base_revision_output",
	  ?_assertEqual(true, is_atom(db_migration:get_base_revision()))},
	 {"check_get_current_head_output",
	  ?_assertEqual(true, is_atom(db_migration:get_current_head()))},
	 %{"check_find_pending_migrations_output",
	 % ?_assertEqual(true, is_list(db_migration:find_pending_migrations()))},
	 {"check_get_revision_tree_output",
	  ?_assertEqual(true, is_list(db_migration:get_revision_tree()))}
	].

check_migration_file_creation_test_() ->
	[%{require, rm, unix},
	 {"check revision file creation",
	  fun () ->
	      {ok, Dir} = file:get_cwd(),
	      code:add_path(Dir),
	      %io:format("path: ~p~n", [Dir]),
	      Filename = db_migration:create_migration_file(),
	      ?assertEqual(true, filelib:is_file(Filename)),
	      ?assertCmd("rm " ++ Filename)
	      %io:format("file: ~p~n", [Filename])
	  end
	 },
	 {"check revision file creation and deletion",
	  fun () ->
	      {ok, Dir} = file:get_cwd(),
	      code:add_path(Dir),
	      Filename = db_migration:create_migration_file(),
	      ?assertCmd("rm " ++ Filename)
	  end
	 }
	].

mnesia_migration_test_() ->
    {
     setup,
     fun() ->
             start_mnesia()
     end,
     fun(_) ->
             stop_mnesia()
     end,
     [
      {inorder,
       [{"Init Migrations should not crash",
         ?_assertEqual(ok, db_migration:init_migrations())},
        {
         "Test migration calculation functions",
         [{inparallel,
           [
            ?_assertEqual(none, db_migration:get_applied_head()),
            ?_assertEqual(test1, db_migration:get_base_revision()),
            ?_assertEqual(test2, db_migration:get_next_revision(test1)),
            ?_assertEqual(test1, db_migration:get_prev_revision(test2)),
            ?_assertEqual([test1, test2, test3],
                          db_migration:find_pending_migrations()),
            ?_assertEqual(1, db_migration:get_count_between_2_revisions(
                               test1, test2))
           ]
          }]
        },
        {"Test apply upgrades",
         [?_assertEqual(ok, db_migration:run_migrations()),
          ?_assertEqual(test3, db_migration:get_applied_head())]
        },
        {"Test apply downgrades",
         [?_assertEqual({error, wrong_number},
                        db_migration:apply_downgrades(5)),
          ?_assertEqual(ok, db_migration:apply_downgrades(1)),
          ?_assertEqual(test2, db_migration:get_applied_head())
         ]},
        {"Test migration run log lifecycle and failure",
         fun() ->
             %% After the downgrade above: head = test2, no test3 attempt yet
             application:set_env(mnesia_migrate, fail_test3, true),
             ?assertError(test3_fail, db_migration:run_migrations()),
             ?assertEqual(test2, db_migration:get_applied_head()),
             Last = db_migration:get_last_migration_run(),
             ?assertEqual(test3, element(5, Last)),
             ?assertEqual(failed, element(7, Last)),
?assertEqual({error, test3_fail}, element(10, Last)),
            ?assertNotEqual(undefined, element(11, Last)),
             %% Retry: only the failing revision is re-attempted, applied
             %% revisions are never re-run
             Test1RowsCount =
                 length([R || R <- db_migration:get_run_log(), element(5, R) =:= test1]),
             ?assertError(test3_fail, db_migration:run_migrations()),
             ?assertEqual(test2, db_migration:get_applied_head()),
             ?assertEqual(
                 Test1RowsCount,
                 length([R || R <- db_migration:get_run_log(), element(5, R) =:= test1]),
                 "test1 must not be re-executed"
             ),
             %% Fix the failure and apply
             application:unset_env(mnesia_migrate, fail_test3),
             ?assertEqual(ok, db_migration:run_migrations()),
             ?assertEqual(test3, db_migration:get_applied_head()),
             LastOk = db_migration:get_last_migration_run(),
             ?assertEqual(test3, element(5, LastOk)),
             ?assertEqual(ok, element(7, LastOk)),
             %% Downgrade direction is recorded too
             ?assertEqual(ok, db_migration:apply_downgrades(1)),
             LastDn = db_migration:get_last_migration_run(),
?assertEqual(test3, element(5, LastDn)),
            ?assertEqual(down, element(6, LastDn)),
             application:unset_env(mnesia_migrate, fail_test3)
         end}
       ]}]
    }.

%% Internal functions

start_mnesia() ->
    ok = mnesia:delete_schema([node()]),
    ok = mnesia:create_schema([node()]),
    {ok, [mnesia]} = application:ensure_all_started(mnesia),
    application:ensure_all_started(mnesia_migrate),
    EbinDir = code:lib_dir(mnesia_migrate) ++ "/test/",
    application:set_env(mnesia_migrate, migration_beam_dir, EbinDir).

stop_mnesia() ->
    ok = application:stop(mnesia_migrate),
    ok = application:stop(mnesia),
    ok = mnesia:delete_schema([node()]).
