open Core.Std
open Async.Std

exception AssertionError of string
exception AssertionFailure of string

type ('suite_params, 'params) t = {
  before_all: unit -> 'suite_params Deferred.t;
  before: 'suite_params -> 'params Deferred.t;
  after: 'params -> unit Deferred.t;
  after_all: 'suite_params -> unit Deferred.t;
  tests: (string * ('params -> unit Deferred.t)) list
}

let create ~before_all
           ~before
           ~after
           ~after_all = {
  before_all;
  before;
  after;
  after_all;
  tests = []
}

let test name f t = { t with tests = (name, f)::t.tests }

let run_test_with_suite t suite_params (name, test) = 
    t.before suite_params            >>= fun params ->
    try_with (fun () -> test params) >>= fun result ->
    t.after params                   >>| fun () ->
    match result with
    | Ok ()   -> printf "%s: SUCCESS\n" name
    | Error e -> match Monitor.extract_exn e with
      | AssertionError s   -> printf "%s: ERROR\n%s\n" name s
      | AssertionFailure s -> printf "%s: FAIL\n%s\n" name s
      | e -> printf "%s: UNKNOWN ERROR\n%s\n" name (Exn.to_string e)

let run t =
  t.before_all () >>= fun suite_params ->
  let tests = List.rev t.tests in
  let run_test = run_test_with_suite t suite_params in
  Deferred.List.iter ~how:`Sequential tests ~f:run_test >>= fun () ->
  t.after_all suite_params

let assert_error : 'a. 'a Deferred.Or_error.t -> string -> unit Deferred.t =
  fun dfd expected_error ->
    dfd
    >>= function
    | Ok _ -> raise (AssertionFailure "Expected to fail")
    | Error e ->
        if Error.to_string_hum e = expected_error then
          return ()
        else
          let error = sprintf "Expected error %s was %s" expected_error (Error.to_string_hum e) in
          raise (AssertionFailure error)

module type Assertable = sig
  include Comparable
  include Sexpable with type t := t
end

module MakeAsserter (M : Assertable) = struct
  let to_string t = t |> M.sexp_of_t |> Sexp.to_string_hum

  let assert_equal : M.t Deferred.Or_error.t -> M.t -> unit Deferred.t =
    fun actual_dfd expected ->
      actual_dfd >>= function
      | Error e -> raise (AssertionError (Core.Error.to_string_hum e))
      | Ok actual ->
        if M.compare actual expected <> 0 then
          let error = sprintf "Expected %s was %s" (to_string expected) (to_string actual) in
          raise (AssertionFailure error)
        else
          return ()
end

module Int    = MakeAsserter(Int)
module Int64  = MakeAsserter(Int64)
module Bool   = MakeAsserter(Bool)
module String = MakeAsserter(String)
