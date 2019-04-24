
mutable struct Sink <: Data.Sink
    dsn::DSN
    table::String
    columns::Vector{Any}
    indcols::Vector{Any}
end

Sink(dsn::DSN, table::AbstractString; append::Bool=false) = Sink(dsn, table, [], [])

# DataStreams interface
function Sink(sch::Data.Schema, ::Type{T}, dsn::DSN, table::AbstractString; append::Bool=false, reference::Vector{UInt8}=UInt8[]) where {T}
    cols = size(sch, 2)
    sink = Sink(dsn, table, Vector{Any}(cols), Vector{Any}(cols))
    !append && ODBC_v06.execute!(dsn, "delete from $table")
    stmt = sink.dsn.stmt_ptr2
    ODBC_v06.execute!(sink.dsn, "select * from $(sink.table)", stmt)
    return sink
end
function Sink(sink, sch::Data.Schema, ::Type{T}; append::Bool=false, reference::Vector{UInt8}=UInt8[]) where {T}
    cols = size(sch, 2)
    resize!(sink.columns, cols)
    resize!(sink.indcols, cols)
    !append && ODBC_v06.execute!(sink.dsn, "delete from $(sink.table)")
    stmt = sink.dsn.stmt_ptr2
    ODBC_v06.execute!(sink.dsn, "select * from $(sink.table)", stmt)
    return sink
end

Data.streamtypes(::Type{ODBC_v06.Sink}) = [Data.Column]

prep!(T, A) = A, 0
prep!(::Type{Union{T, Missing}}, A) where {T} = T[ifelse(ismissing(x), zero(T), x) for x in A]
prep!(::Type{Union{Dates.Date, Missing}}, A) = ODBC_v06.API.SQLDate[ismissing(x) ? ODBC_v06.API.SQLDate() : ODBC_v06.API.SQLDate(x) for x in A], 0
prep!(::Type{Union{Dates.DateTime, Missing}}, A) = ODBC_v06.API.SQLTimestamp[ismissing(x) ? ODBC_v06.API.SQLTimestamp() : ODBC_v06.API.SQLTimestamp(x) for x in A], 0
prep!(::Type{Union{Float64, Missing}}, A) = Float64[ismissing(x) ? 0.0 : Float64(x) for x in A], 0

getptrlen(x::AbstractString) = pointer(Vector{UIn8}(x)), length(x), UInt8[]
getptrlen(x::WeakRefString{T}) where {T} = convert(Ptr{UInt8}, x.ptr), codeunits2bytes(T, x.len), UInt8[]
getptrlen(x::Missing) = convert(Ptr{UInt8}, C_NULL), 0, UInt8[]
function getptrlen(x::CategoricalArrays.CategoricalValue)
    ref = Vector{UInt8}(String(x))
    return pointer(ref), length(ref), ref
end

prep!(::Type{T}, A) where {T <: AbstractString} = _prep!(T, A)
prep!(::Type{Union{T, Missing}}, A) where {T <: AbstractString} = _prep!(T, A)
prep!(::Type{T}, A) where {T <: CategoricalValue} = _prep!(T, A)
prep!(::Type{Union{T, Missing}}, A) where {T <: CategoricalValue} = _prep!(T, A)

function _prep!(T, column)
    maxlen = maximum(ODBC_v06.clength, column)
    data = zeros(UInt8, maxlen * length(column))
    ind = 1
    for i = 1:length(column)
        ptr, len, ref = getptrlen(column[i])
        unsafe_copy!(pointer(data, ind), ptr, len)
        ind += maxlen
    end
    return data, maxlen
end

function prep!(column::T, col, columns, indcols) where {T}
    columns[col], maxlen = prep!(eltype(T), column)
    indcols[col] = ODBC_v06.API.SQLLEN[clength(x) for x in column]
    return length(column), maxlen
end

getCtype(::Type{T}) where {T} = get(ODBC_v06.API.julia2C, T, ODBC_v06.API.SQL_C_CHAR)
getCtype(::Type{Union{T, Missing}}) where {T} = get(ODBC_v06.API.julia2C, T, ODBC_v06.API.SQL_C_CHAR)
getCtype(::Type{Vector{T}}) where {T} = get(ODBC_v06.API.julia2C, T, ODBC_v06.API.SQL_C_CHAR)
getCtype(::Type{Vector{Union{T, Missing}}}) where {T} = get(ODBC_v06.API.julia2C, T, ODBC_v06.API.SQL_C_CHAR)

function Data.streamto!(sink::ODBC_v06.Sink, ::Type{Data.Column}, column::T, col) where {T}
    stmt = sink.dsn.stmt_ptr2
    rows, len = ODBC_v06.prep!(column, col, sink.columns, sink.indcols)
    ODBC_v06.@CHECK stmt ODBC_v06.API.SQL_HANDLE_STMT ODBC_v06.API.SQLBindCols(stmt, col, getCtype(T), sink.columns[col], len, sink.indcols[col])
    if col == length(sink.columns)
        ODBC_v06.API.SQLSetStmtAttr(stmt, ODBC_v06.API.SQL_ATTR_ROW_ARRAY_SIZE, rows, ODBC_v06.API.SQL_IS_UINTEGER)
        ODBC_v06.@CHECK stmt ODBC_v06.API.SQL_HANDLE_STMT ODBC_v06.API.SQLBulkOperations(stmt, ODBC_v06.API.SQL_ADD)
    end
    return rows
end

function load(dsn::DSN, table::AbstractString, ::Type{T}, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}()) where {T}
    sink = Data.stream!(T(args...), ODBC_v06.Sink, dsn, table; append=append, transforms=transforms)
    return Data.close!(sink)
end
function load(dsn::DSN, table::AbstractString, source; append::Bool=false, transforms::Dict=Dict{Int,Function}())
    sink = Data.stream!(source, ODBC_v06.Sink, dsn, table; append=append, transforms=transforms)
    return Data.close!(sink)
end

load(sink::Sink, ::Type{T}, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}()) where {T} = (sink = Data.stream!(T(args...), sink; append=append, transforms=transforms); return Data.close!(sink))
load(sink::Sink, source; append::Bool=false, transforms::Dict=Dict{Int,Function}()) = (sink = Data.stream!(source, sink; append=append, transforms=transforms); return Data.close!(sink))

# function Data.stream!(source, ::Type{Data.Column}, sink::ODBC_v06.Sink, append::Bool=false)
#     Data.types(source) == Data.types(sink) || throw(ArgumentError("schema mismatch: \n$(Data.schema(source))\nvs.\n$(Data.schema(sink))"))
#     rows, cols = size(source)
#     Data.isdone(source, 1, 1) && return sink
#     ODBC_v06.execute!(sink.dsn, "select * from $(sink.table)")
#     stmt = sink.dsn.stmt_ptr
#     types = Data.types(source)
#     columns = Vector{Any}(cols)
#     indcols = Array{Vector{ODBC_v06.API.SQLLEN}}(cols)
#     row = 0
#     # get the column names for a table from the DB to generate the insert into sql statement
#     # might have to try quoting
#     # SQLPrepare (hdlStmt, (SQLTCHAR*)"INSERT INTO customers (CustID, CustName,  Phone_Number) VALUES(?,?,?)", SQL_NTS) ;
#     try
#         # SQLSetConnectAttr(hdlDbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, SQL_NTS)
#         while !Data.isdone(source, row+1, cols+1)
#
#             for col = 1:cols
#                 T = types[col]
#                 # SQLBindParameter(hdlStmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, (SQLPOINTER)custIDs, sizeof(SQLINTEGER) , NULL);
#                 rows, cT = ODBC_v06.bindcolumn!(source, T, col, columns, indcols)
#                 ret = ODBC_v06.API.SQLBindCols(stmt, col, cT, pointer(columns[col]), sizeof(eltype(columns[col])), indcols[col])
#                 println("$col: $ret")
#             end
#             ODBC_v06.API.SQLSetStmtAttr(stmt, ODBC_v06.API.SQL_ATTR_ROW_ARRAY_SIZE, rows, ODBC_v06.API.SQL_IS_UINTEGER)
#             # SQLSetStmtAttr( hdlStmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)NUM_ENTRIES, 0 );
#             # ret = SQLExecute(hdlStmt);
#             ODBC_v06.@CHECK stmt ODBC_v06.API.SQL_HANDLE_STMT ODBC_v06.API.SQLBulkOperations(stmt, ODBC_v06.API.SQL_ADD)
#             row += rows
#         end
#         # SQLEndTran(SQL_HANDLE_DBC, hdlDbc, SQL_COMMIT);
#     # finally
#         # SQLSetConnectAttr(hdlDbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_ON, SQL_NTS);
#     end
#     Data.setrows!(source, row)
#     return sink
# end
