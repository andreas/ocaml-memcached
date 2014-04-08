open Core.Std
open Async.Std

module type S = sig
  type t
  type value

  type opcode =
      Get
    | Set
    | Add
    | Replace
    | Delete
    | Increment
    | Decrement
    | Quit
    | Flush
    | GetQ
    | NoOp
    | Version
    | GetK
    | GetKQ
    | Append
    | Prepend
    | Stat
    | SetQ
    | AddQ
    | ReplaceQ
    | DeleteQ
    | IncrementQ
    | DecrementQ
    | QuitQ
    | FlushQ
    | AppendQ
    | PrependQ

  type status =
      NoError
    | KeyNotFound
    | KeyExists
    | ValueTooLarge
    | InvalidArguments
    | ItemNotStored
    | IncrDecrOnNonNumericValue
    | UnknownCommand
    | OutOfMemory

  type data_type = RawBytes

  type response = {
    opcode : opcode;
    key_length : int;
    extras_length : int;
    data_type : data_type;
    status : status;
    total_body_length : int32;
    opaque : string;
    cas : string;
    extras : string;
    key : string;
    value : string;
  }

  val connect : host:string -> port:int -> t Deferred.t

  val command :
    ?data_type:data_type ->
    ?opaque:Int32.t ->
    ?cas:Int64.t ->
    ?value:value option ->
    ?extras:string option ->
    ?key:string option ->
    t ->
    opcode:opcode ->
    response Deferred.Or_error.t

  val get :
    t ->
    key:string ->
    value Deferred.Or_error.t

  val set :
    ?flags:Int32.t ->
    ?expiry:Int32.t ->
    t ->
    key:string ->
    value:value ->
    bool Deferred.Or_error.t

  val add :
    ?flags:Int32.t ->
    ?expiry:Int32.t ->
    t ->
    key:string ->
    value:value ->
    bool Deferred.Or_error.t

  val replace :
    ?flags:Int32.t ->
    ?expiry:Int32.t ->
    t ->
    key:string ->
    value:value ->
    bool Deferred.Or_error.t

  val delete :
    t ->
    key:string ->
    bool Deferred.Or_error.t

  val incr :
    ?expiry:int32 ->
    t ->
    key:string ->
    initial:int64 ->
    delta:int64 ->
    int64 Deferred.Or_error.t

  val decr :
    ?expiry:int32 ->
    t ->
    key:string ->
    initial:int64 ->
    delta:int64 ->
    int64 Deferred.Or_error.t

  val prepend :
    t ->
    key:string ->
    value:value ->
    bool Deferred.Or_error.t

  val append :
    t ->
    key:string ->
    value:value ->
    bool Deferred.Or_error.t

  val flush :
    ?expiry:int32 ->
    t ->
    bool Deferred.Or_error.t

  val close : t -> unit Deferred.t
end

module type Value = sig
    type t
    val to_string: t -> string
    val of_string: string -> t
end

module Make (Value : Value) : S with type value = Value.t
