__precompile__(true)
module ODBC_v06

using DataStreams, Missings, CategoricalArrays, WeakRefStrings, DataFrames
import Compat: Sys

export Data, DataFrame, odbcdf

if VERSION < v"0.7.0-DEV.2575"
    const Dates = Base.Dates
else
    import Dates
end

include("API.jl")

"just a block of memory; T is the element type, `len` is total # of **bytes** pointed to, and `elsize` is size of each element"
mutable struct Block{T}
    ptr::Ptr{T}    # pointer to a block of memory
    len::Int       # total # of bytes in block
    elsize::Int    # size between elements in bytes
end

"""
Block allocator:
    -Takes an element type, and number of elements to allocate in a linear block
    -Optionally specify an extra dimension of elements that make up each element (i.e. container types)
"""
function Block(::Type{T}, elements::Int, extradim::Integer=1) where {T}
    len = sizeof(T) * elements * extradim
    block = Block{T}(convert(Ptr{T}, Libc.malloc(len)), len, sizeof(T) * extradim)
    finalizer(block, x->Libc.free(x.ptr))
    return block
end

# used for getting messages back from ODBC driver manager; SQLDrivers, SQLError, etc.
Base.string(block::Block, len::Integer) = String(transcode(UInt8, unsafe_wrap(Array, block.ptr, len, false)))

struct ODBCError <: Exception
    msg::String
end

const BUFLEN = 1024

function ODBCError(handle::Ptr{Void}, handletype::Int16)
    i = Int16(1)
    state = ODBC_v06.Block(ODBC_v06.API.SQLWCHAR, 6)
    native = Ref{ODBC_v06.API.SQLINTEGER}()
    error_msg = ODBC_v06.Block(ODBC_v06.API.SQLWCHAR, BUFLEN)
    msg_length = Ref{ODBC_v06.API.SQLSMALLINT}()
    while ODBC_v06.API.SQLGetDiagRec(handletype, handle, i, state.ptr, native, error_msg.ptr, BUFLEN, msg_length) == ODBC_v06.API.SQL_SUCCESS
        st  = string(state, 5)
        msg = string(error_msg, msg_length[])
        println("[ODBC] $st: $msg")
        i += 1
    end
    return true
end

#Macros to to check if a function returned a success value or not
macro CHECK(handle, handletype, func)
    str = string(func)
    esc(quote
        ret = $func
        ret != ODBC_v06.API.SQL_SUCCESS && ret != ODBC_v06.API.SQL_SUCCESS_WITH_INFO && ODBCError($handle, $handletype) &&
            throw(ODBCError("$($str) failed; return code: $ret => $(ODBC_v06.API.RETURN_VALUES[ret])"))
        nothing
    end)
end

Base.@deprecate listdrivers ODBC_v06.drivers
Base.@deprecate listdsns ODBC_v06.dsns

"List ODBC drivers that have been installed and registered"
function drivers()
    descriptions = String[]
    attributes   = String[]
    driver_desc = Block(ODBC_v06.API.SQLWCHAR, BUFLEN)
    desc_length = Ref{ODBC_v06.API.SQLSMALLINT}()
    driver_attr = Block(ODBC_v06.API.SQLWCHAR, BUFLEN)
    attr_length = Ref{ODBC_v06.API.SQLSMALLINT}()
    dir = ODBC_v06.API.SQL_FETCH_FIRST
    while ODBC_v06.API.SQLDrivers(ENV, dir, driver_desc.ptr, BUFLEN, desc_length, driver_attr.ptr, BUFLEN, attr_length) == ODBC_v06.API.SQL_SUCCESS
        push!(descriptions, string(driver_desc, desc_length[]))
        push!(attributes,   string(driver_attr, attr_length[]))
        dir = ODBC_v06.API.SQL_FETCH_NEXT
    end
    return [descriptions attributes]
end

"List ODBC DSNs, both user and system, that have been previously defined"
function dsns()
    descriptions = String[]
    attributes   = String[]
    dsn_desc    = Block(ODBC_v06.API.SQLWCHAR, BUFLEN)
    desc_length = Ref{ODBC_v06.API.SQLSMALLINT}()
    dsn_attr    = Block(ODBC_v06.API.SQLWCHAR, BUFLEN)
    attr_length = Ref{ODBC_v06.API.SQLSMALLINT}()
    dir = ODBC_v06.API.SQL_FETCH_FIRST
    while ODBC_v06.API.SQLDataSources(ENV, dir, dsn_desc.ptr, BUFLEN, desc_length, dsn_attr.ptr, BUFLEN, attr_length) == ODBC_v06.API.SQL_SUCCESS
        push!(descriptions, string(dsn_desc, desc_length[]))
        push!(attributes,   string(dsn_attr, attr_length[]))
        dir = ODBC_v06.API.SQL_FETCH_NEXT
    end
    return [descriptions attributes]
end

"""
A DSN represents an established ODBC connection.
It is passed to most other ODBC methods as a first argument
"""
mutable struct DSN
    dsn::String
    dbc_ptr::Ptr{Void}
    stmt_ptr::Ptr{Void}
    stmt_ptr2::Ptr{Void}
end

Base.show(io::IO,conn::DSN) = print(io, "ODBC_v06.DSN($(conn.dsn))")

const dsn = DSN("", C_NULL, C_NULL, C_NULL)

"""
Construct a `DSN` type by connecting to a valid ODBC DSN or by specifying a valid connection string.
Takes optional 2nd and 3rd arguments for `username` and `password`, respectively.
1st argument `dsn` can be either the name of a pre-defined ODBC DSN or a valid connection string.
A great resource for building valid connection strings is [http://www.connectionstrings.com/](http://www.connectionstrings.com/).
"""
function DSN(connectionstring::AbstractString, username::AbstractString=String(""), password::AbstractString=String(""); prompt::Bool=true)
    dbc = ODBC_v06.ODBCAllocHandle(ODBC_v06.API.SQL_HANDLE_DBC, ODBC_v06.ENV)
    dsns = ODBC_v06.dsns()
    found = false
    for d in dsns[:,1]
        connectionstring == d && (found = true)
    end
    if found
        @CHECK dbc ODBC_v06.API.SQL_HANDLE_DBC ODBC_v06.API.SQLConnect(dbc, connectionstring, username, password)
    else
        connectionstring = ODBCDriverConnect!(dbc, connectionstring, prompt)
    end
    stmt = ODBCAllocHandle(ODBC_v06.API.SQL_HANDLE_STMT, dbc)
    stmt2 = ODBCAllocHandle(ODBC_v06.API.SQL_HANDLE_STMT, dbc)
    global dsn
    dsn.dsn = connectionstring
    dsn.dbc_ptr = dbc
    dsn.stmt_ptr = stmt
    dsn.stmt_ptr2 = stmt2
    return DSN(connectionstring, dbc, stmt, stmt2)
end

"disconnect a connected `DSN`"
function disconnect!(conn::DSN)
    ODBCFreeStmt!(conn.stmt_ptr)
    ODBCFreeStmt!(conn.stmt_ptr2)
    ODBC_v06.API.SQLDisconnect(conn.dbc_ptr)
    return nothing
end

mutable struct Statement
    dsn::DSN
    stmt::Ptr{Void}
    query::String
    task::Task
end

"An `ODBC_v06.Source` type executes a `query` string upon construction and prepares data for streaming to an appropriate `Data.Sink`"
mutable struct Source{T} <: Data.Source
    schema::Data.Schema
    dsn::DSN
    query::String
    columns::T
    status::Int
    rowsfetched::Ref{ODBC_v06.API.SQLLEN}
    rowoffset::Int
    boundcols::Vector{Any}
    indcols::Vector{Vector{ODBC_v06.API.SQLLEN}}
    sizes::Vector{ODBC_v06.API.SQLULEN}
    ctypes::Vector{ODBC_v06.API.SQLSMALLINT}
    jltypes::Vector{Type}
    supportsreset::Bool
end

Base.show(io::IO, source::Source) = print(io, "ODBC_v06.Source:\n\tDSN: $(source.dsn)\n\tstatus: $(source.status)\n\tschema: $(source.schema)")

include("Source.jl")
include("Sink.jl")
include("sqlreplmode.jl")

function __init__()
    global const ENV = ODBC_v06.ODBCAllocHandle(ODBC_v06.API.SQL_HANDLE_ENV, ODBC_v06.API.SQL_NULL_HANDLE)
    global const DECZERO = Float64(0)
    toggle_sql_repl()
end

# used to 'clear' a statement of bound columns, resultsets,
# and other bound parameters in preparation for a subsequent query
function ODBCFreeStmt!(stmt)
    ODBC_v06.API.SQLFreeStmt(stmt, ODBC_v06.API.SQL_CLOSE)
    ODBC_v06.API.SQLFreeStmt(stmt, ODBC_v06.API.SQL_UNBIND)
    ODBC_v06.API.SQLFreeStmt(stmt, ODBC_v06.API.SQL_RESET_PARAMS)
end

end #ODBC module
