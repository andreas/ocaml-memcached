open Core.Std
open Async
open Async.Std
open Async_unix

module type Value = sig
    type t
    val to_string: t -> string
    val of_string: string -> t
end

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

module Make(Value : Value) = struct
  type t = {
    reader: Reader.t;
    writer: Writer.t;
    header_buffer: string
  }

  type value = Value.t

  type opcode = 
    | Get
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

  let opcode_from_int = function
    | 0x00 -> Get
    | 0x01 -> Set
    | 0x02 -> Add
    | 0x03 -> Replace
    | 0x04 -> Delete
    | 0x05 -> Increment
    | 0x06 -> Decrement
    | 0x07 -> Quit
    | 0x08 -> Flush
    | 0x09 -> GetQ
    | 0x0A -> NoOp
    | 0x0B -> Version
    | 0x0C -> GetK
    | 0x0D -> GetKQ
    | 0x0E -> Append
    | 0x0F -> Prepend
    | 0x10 -> Stat
    | 0x11 -> SetQ
    | 0x12 -> AddQ
    | 0x13 -> ReplaceQ
    | 0x14 -> DeleteQ
    | 0x15 -> IncrementQ
    | 0x16 -> DecrementQ
    | 0x17 -> QuitQ
    | 0x18 -> FlushQ
    | 0x19 -> AppendQ
    | 0x1A -> PrependQ
    | i    -> failwith (sprintf "Unknown opcode %d" i)

  let opcode_to_int = function
    | Get        -> 0x00
    | Set        -> 0x01
    | Add        -> 0x02
    | Replace    -> 0x03
    | Delete     -> 0x04
    | Increment  -> 0x05
    | Decrement  -> 0x06
    | Quit       -> 0x07
    | Flush      -> 0x08
    | GetQ       -> 0x09
    | NoOp       -> 0x0A
    | Version    -> 0x0B
    | GetK       -> 0x0C
    | GetKQ      -> 0x0D
    | Append     -> 0x0E
    | Prepend    -> 0x0F
    | Stat       -> 0x10
    | SetQ       -> 0x11
    | AddQ       -> 0x12
    | ReplaceQ   -> 0x13
    | DeleteQ    -> 0x14
    | IncrementQ -> 0x15
    | DecrementQ -> 0x16
    | QuitQ      -> 0x17
    | FlushQ     -> 0x18
    | AppendQ    -> 0x19
    | PrependQ   -> 0x1A

  type status =
    | NoError
    | KeyNotFound
    | KeyExists
    | ValueTooLarge
    | InvalidArguments
    | ItemNotStored
    | IncrDecrOnNonNumericValue
    | UnknownCommand
    | OutOfMemory
    
  let status_from_int = function
    | 0x0000 -> NoError
    | 0x0001 -> KeyNotFound
    | 0x0002 -> KeyExists
    | 0x0003 -> ValueTooLarge
    | 0x0004 -> InvalidArguments
    | 0x0005 -> ItemNotStored
    | 0x0006 -> IncrDecrOnNonNumericValue
    | 0x0081 -> UnknownCommand
    | 0x0082 -> OutOfMemory

  let status_to_int = function
    | NoError                   -> 0x0000
    | KeyNotFound               -> 0x0001
    | KeyExists                 -> 0x0002
    | ValueTooLarge             -> 0x0003
    | InvalidArguments          -> 0x0004
    | ItemNotStored             -> 0x0005
    | IncrDecrOnNonNumericValue -> 0x0006
    | UnknownCommand            -> 0x0081
    | OutOfMemory               -> 0x0082

  let status_to_string = function
    | NoError                   -> "NoError"
    | KeyNotFound               -> "KeyNotFound"
    | KeyExists                 -> "KeyExists"
    | ValueTooLarge             -> "ValueTooLarge"
    | InvalidArguments          -> "InvalidArguments"
    | ItemNotStored             -> "ItemNotStored"
    | IncrDecrOnNonNumericValue -> "IncrDecrOnNonNumericValue"
    | UnknownCommand            -> "UnknownCommand"
    | OutOfMemory               -> "OutOfMemory"

  type data_type =
    | RawBytes

  let data_type_from_int = function
    | 0x00 -> RawBytes

  let data_type_to_int = function
    | RawBytes -> 0x00

  type response = {
    opcode: opcode;
    key_length: int;
    extras_length: int;
    data_type: data_type;
    status: status;
    total_body_length: int32;
    opaque: string;
    cas: string;
    extras: string;
    key: string;
    value: string;
  }

  let int32_of_int_exn i = Option.value_exn(Int32.of_int i)
  let int32_to_int_exn i = Option.value_exn(Int32.to_int i)

  let value_length t =
    let key_length    = int32_of_int_exn t.key_length in
    let extras_length = int32_of_int_exn t.extras_length in
    let value_length  = Int32.(t.total_body_length - key_length - extras_length) in
    int32_to_int_exn value_length

  let header_length = 24

  let parse_response_header bits = bitmatch (Bitstring.bitstring_of_string bits) with
    | { 0x81               : 8;
        opcode             : 8;
        key_length         : 2*8 : unsigned;
        extras_length      : 8 : unsigned;
        data_type          : 8;
        status             : 2*8;
        total_body_length  : 4*8 : unsigned;
        opaque             : 4*8 : string;
        cas                : 8*8 : string
      } -> {
        opcode = opcode_from_int opcode;
        key_length;
        extras_length;
        data_type = data_type_from_int data_type;
        status = status_from_int status;
        total_body_length = total_body_length;
        opaque;
        cas;
        extras = "";
        key = "";
        value = "";
      }

  let parse_response_body t bits = bitmatch (Bitstring.bitstring_of_string bits) with
    | { extras : 8*t.extras_length  : string;
        key    : 8*t.key_length     : string;
        value  : 8*(value_length t) : string
      } -> { t with
        extras;
        key;
        value;
      }

  let response_from_reader reader header_buffer =
    Reader.really_read reader ~len:header_length header_buffer >>= function
    | `Eof i -> return (Or_error.error_string (sprintf "Failed to read header: %d" i))
    | `Ok ->
        let response = parse_response_header header_buffer in
        if response.total_body_length = 0l then
          return (Ok response)
        else
          let body_length = int32_to_int_exn response.total_body_length in
          let body_buffer = String.create body_length in
          Reader.really_read reader ~len:body_length body_buffer >>| function
          | `Eof i -> Or_error.error_string "Failed to read body"
          | `Ok ->
              Ok (parse_response_body response body_buffer)

  let string_option_length = Option.value_map ~f:String.length ~default:0

  let make_request_header opcode key value extras data_type opaque cas =
    let key_length    = string_option_length key    in
    let extras_length = string_option_length extras in
    let value_length  = string_option_length value  in
    let total_body_length = int32_of_int_exn (key_length + extras_length + value_length) in
    BITSTRING {
      0x80                       : 8;
      opcode_to_int opcode       : 8;
      key_length                 : 2*8;
      extras_length              : 8;
      data_type_to_int data_type : 8;
      0                          : 16;
      total_body_length          : 4*8;
      opaque                     : 4*8;
      cas                        : 8*8
    }

  let connect ~host ~port =
    Tcp.to_host_and_port host port
    |> Tcp.connect
    >>| fun (_, reader, writer) ->
      { reader;
        writer;
        header_buffer=String.create header_length;
      }

  let command ?(data_type=RawBytes) ?(opaque=0l) ?(cas=0L)
  ?(value=None) ?(extras=None) ?(key=None) t ~opcode =
    try_with (fun () ->
      Stream.iter (Monitor.errors (Writer.monitor t.writer)) ~f:raise;
      let value_str = Option.map value ~f:Value.to_string in
      make_request_header opcode key value_str extras data_type opaque cas
      |> Bitstring.string_of_bitstring
      |> Writer.write t.writer;
      Option.iter extras ~f:(Writer.write t.writer);
      Option.iter key ~f:(Writer.write t.writer);
      Option.iter value_str ~f:(Writer.write t.writer);
      response_from_reader t.reader t.header_buffer
    ) >>| function
        | Ok response -> response
        | Error exn   -> Or_error.of_exn exn

  let set_add_replace opcode ?(flags=0l) ?(expiry=0l) t ~key ~value =
    let extras_bits = BITSTRING { flags: 4*8; expiry: 4*8 } in
    let extras      = Bitstring.string_of_bitstring extras_bits in
    command t ~opcode:opcode ~key:(Some key) ~value:(Some value) ~extras:(Some extras) >>|? fun response ->
    response.status = NoError

  let set     = set_add_replace Set
  let add     = set_add_replace Add
  let replace = set_add_replace Replace

  let delete t ~key =
    command t ~opcode:Delete ~key:(Some key) >>|? fun response ->
    response.status = NoError

  let get t ~key =
    command t ~opcode:Get ~key:(Some key) >>=? fun response ->
    if response.status = NoError then
      return (Ok (Value.of_string response.value))
    else
      return (Or_error.error_string "Not found")

  let arith t opcode key initial delta expiry =
    let extras_bits = BITSTRING { delta: 8*8; initial: 8*8; expiry: 4*8 } in
    let extras = Bitstring.string_of_bitstring extras_bits in
    command t ~opcode ~key:(Some key) ~extras:(Some extras) >>=? fun response ->
    if response.status = NoError then
      bitmatch (Bitstring.bitstring_of_string response.value) with
      { value :  8*8 } -> return (Ok value)
    else
      return (Or_error.error_string (sprintf "Memcached status %s\n" (status_to_string
      response.status)))

  let incr ?(expiry=0l) t ~key ~initial ~delta =
    arith t Increment key initial delta expiry

  let decr ?(expiry=0l) t ~key ~initial ~delta =
    arith t Decrement key initial delta expiry

  let prepend t ~key ~value =
    command t ~opcode:Prepend ~key:(Some key) ~value:(Some value) >>|? fun response ->
    response.status = NoError

  let append t ~key ~value =
    command t ~opcode:Append ~key:(Some key) ~value:(Some value) >>|? fun response ->
    response.status = NoError

  let flush ?(expiry=0l) t =
    let extras_bits = BITSTRING { expiry: 4*8 } in
    let extras = Bitstring.string_of_bitstring extras_bits in
    command t ~opcode:Flush ~extras:(Some extras) >>|? fun response ->
    response.status = NoError

  let close t =
    Writer.close t.writer >>= fun () ->
    Reader.close t.reader
end
