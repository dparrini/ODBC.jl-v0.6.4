# "Allocate ODBC handles for interacting with the ODBC Driver Manager"
function ODBCAllocHandle(handletype, parenthandle)
    handle = Ref{Ptr{Void}}()
    ODBC_v06.API.SQLAllocHandle(handletype, parenthandle, handle)
    handle = handle[]
    if handletype == ODBC_v06.API.SQL_HANDLE_ENV
        ODBC_v06.API.SQLSetEnvAttr(handle, ODBC_v06.API.SQL_ATTR_ODBC_VERSION, ODBC_v06.API.SQL_OV_ODBC3)
    end
    return handle
end

# "Alternative connect function that allows user to create datasources on the fly through opening the ODBC admin"
function ODBCDriverConnect!(dbc::Ptr{Void}, conn_string, prompt::Bool)
    @static if is_windows()
        driver_prompt = prompt ? ODBC_v06.API.SQL_DRIVER_PROMPT : ODBC_v06.API.SQL_DRIVER_NOPROMPT
        window_handle = prompt ? ccall((:GetForegroundWindow, :user32), Ptr{Void}, () ) : C_NULL
    else
        driver_prompt = ODBC_v06.API.SQL_DRIVER_NOPROMPT
        window_handle = C_NULL
    end
    out_conn = Block(ODBC_v06.API.SQLWCHAR, BUFLEN)
    out_buff = Ref{Int16}()
    @CHECK dbc ODBC_v06.API.SQL_HANDLE_DBC ODBC_v06.API.SQLDriverConnect(dbc, window_handle, conn_string, out_conn.ptr, BUFLEN, out_buff, driver_prompt)
    connection_string = string(out_conn, out_buff[])
    return connection_string
end

"`ODBC_v06.prepare` prepares an SQL statement to be executed"
function prepare(dsn::DSN, query::AbstractString)
    stmt = ODBCAllocHandle(ODBC_v06.API.SQL_HANDLE_STMT, dsn.dbc_ptr)
    ODBC_v06.@CHECK stmt ODBC_v06.API.SQL_HANDLE_STMT ODBC_v06.API.SQLPrepare(stmt, query)
    return Statement(dsn, stmt, query, Task(1))
end

cast(x) = x
cast(x::Dates.Date) = ODBC_v06.API.SQLDate(x)
cast(x::Dates.DateTime) = ODBC_v06.API.SQLTimestamp(x)
cast(x::String) = WeakRefString(pointer(x), sizeof(x))

getpointer(::Type{T}, A, i) where {T} = unsafe_load(Ptr{Ptr{Void}}(pointer(A, i)))
getpointer(::Type{WeakRefString{T}}, A, i) where {T} = convert(Ptr{Void}, A[i].ptr)
getpointer(::Type{String}, A, i) = convert(Ptr{Void}, pointer(Vector{UInt8}(A[i])))

sqllength(x) = 1
sqllength(x::AbstractString) = length(x)
sqllength(x::Vector{UInt8}) = length(x)
sqllength(x::WeakRefString) = x.len
sqllength(x::ODBC_v06.API.SQLDate) = 10
sqllength(x::Union{ODBC_v06.API.SQLTime,ODBC_v06.API.SQLTimestamp}) = length(string(x))

clength(x) = 1
clength(x::AbstractString) = length(x)
clength(x::Vector{UInt8}) = length(x)
clength(x::WeakRefString{T}) where {T} = codeunits2bytes(T, x.len)
clength(x::CategoricalArrays.CategoricalValue) = length(String(x))
clength(x::Missing) = ODBC_v06.API.SQL_NULL_DATA

digits(x) = 0
digits(x::ODBC_v06.API.SQLTimestamp) = length(string(x.fraction * 1000000))

function execute!(statement::Statement, values)
    stmt = statement.stmt
    values2 = Any[cast(x) for x in values]
    pointers = Ptr[]
    types = map(typeof, values2)
    for (i, v) in enumerate(values2)
        if ismissing(v)
            ODBC_v06.@CHECK stmt ODBC_v06.API.SQL_HANDLE_STMT ODBC_v06.API.SQLBindParameter(stmt, i, ODBC_v06.API.SQL_PARAM_INPUT,
                ODBC_v06.API.SQL_C_CHAR, ODBC_v06.API.SQL_CHAR, 0, 0, C_NULL, 0, Ref(ODBC_v06.API.SQL_NULL_DATA))
        else
            ctype, sqltype = ODBC_v06.API.julia2C[types[i]], ODBC_v06.API.julia2SQL[types[i]]
            csize, len, dgts = sqllength(v), clength(v), digits(v)
            ptr = getpointer(types[i], values2, i)
            # println("ctype: $ctype, sqltype: $sqltype, digits: $dgts, len: $len, csize: $csize")
            push!(pointers, ptr)
            ODBC_v06.@CHECK stmt ODBC_v06.API.SQL_HANDLE_STMT ODBC_v06.API.SQLBindParameter(stmt, i, ODBC_v06.API.SQL_PARAM_INPUT,
                ctype, sqltype, csize, dgts, ptr, len, Ref(len))
        end
    end
    execute!(statement)
    return
end

function execute!(statement::Statement)
    stmt = statement.stmt
    ODBC_v06.@CHECK stmt ODBC_v06.API.SQL_HANDLE_STMT ODBC_v06.API.SQLExecute(stmt)
    return
end

"`ODBC_v06.execute!` is a minimal method for just executing an SQL `query` string. No results are checked for or returned."
function execute!(dsn::DSN, query::AbstractString, stmt=dsn.stmt_ptr)
    ODBC_v06.ODBCFreeStmt!(stmt)
    ODBC_v06.@CHECK stmt ODBC_v06.API.SQL_HANDLE_STMT ODBC_v06.API.SQLExecDirect(stmt, query)
    return
end

"""
`ODBC_v06.Source` constructs a valid `Data.Source` type that executes an SQL `query` string for the `dsn` ODBC DSN.
Results are checked for and an `ODBC_v06.ResultBlock` is allocated to prepare for fetching the resultset.
"""
function Source(dsn::DSN, query::AbstractString; weakrefstrings::Bool=true, noquery::Bool=false)
    stmt = dsn.stmt_ptr
    noquery || ODBC_v06.ODBCFreeStmt!(stmt)
    supportsreset = ODBC_v06.API.SQLSetStmtAttr(stmt, ODBC_v06.API.SQL_ATTR_CURSOR_SCROLLABLE, ODBC_v06.API.SQL_SCROLLABLE, ODBC_v06.API.SQL_IS_INTEGER)
    supportsreset &= ODBC_v06.API.SQLSetStmtAttr(stmt, ODBC_v06.API.SQL_ATTR_CURSOR_TYPE, ODBC_v06.API.SQL_CURSOR_STATIC, ODBC_v06.API.SQL_IS_INTEGER)
    noquery || (ODBC_v06.@CHECK stmt ODBC_v06.API.SQL_HANDLE_STMT ODBC_v06.API.SQLExecDirect(stmt, query))
    rows, cols = Ref{Int}(), Ref{Int16}()
    ODBC_v06.API.SQLNumResultCols(stmt, cols)
    ODBC_v06.API.SQLRowCount(stmt, rows)
    rows, cols = rows[], cols[]
    #Allocate arrays to hold each column's metadata
    cnames = Array{String}(cols)
    ctypes, csizes = Array{ODBC_v06.API.SQLSMALLINT}(cols), Array{ODBC_v06.API.SQLULEN}(cols)
    cdigits, cnulls = Array{ODBC_v06.API.SQLSMALLINT}(cols), Array{ODBC_v06.API.SQLSMALLINT}(cols)
    juliatypes = Array{Type}(cols)
    alloctypes = Array{DataType}(cols)
    longtexts = Array{Bool}(cols)
    longtext = false
    #Allocate space for and fetch the name, type, size, etc. for each column
    len, dt, csize = Ref{ODBC_v06.API.SQLSMALLINT}(), Ref{ODBC_v06.API.SQLSMALLINT}(), Ref{ODBC_v06.API.SQLULEN}()
    digits, missing = Ref{ODBC_v06.API.SQLSMALLINT}(), Ref{ODBC_v06.API.SQLSMALLINT}()
    cname = ODBC_v06.Block(ODBC_v06.API.SQLWCHAR, ODBC_v06.BUFLEN)
    for x = 1:cols
        ODBC_v06.API.SQLDescribeCol(stmt, x, cname.ptr, ODBC_v06.BUFLEN, len, dt, csize, digits, missing)
        cnames[x] = string(cname, len[])
        t = dt[]
        ctypes[x], csizes[x], cdigits[x], cnulls[x] = t, csize[], digits[], missing[]
        alloctypes[x], juliatypes[x], longtexts[x] = ODBC_v06.API.SQL2Julia[t]
        longtext |= longtexts[x]
    end
    if !weakrefstrings
        foreach(i->juliatypes[i] <: Union{WeakRefString, Missing} && setindex!(juliatypes, Union{String, Missing}, i), 1:length(juliatypes))
    end
    # Determine fetch strategy
    # rows might be -1 (dbms doesn't return total rows in resultset), 0 (empty resultset), or 1+
    if longtext
        rowset = allocsize = 1
    elseif rows > -1
        # rowset = min(rows, ODBC_v06.API.MAXFETCHSIZE)
        allocsize = rowset = rows
    else
        rowset = allocsize = 1
    end
    ODBC_v06.API.SQLSetStmtAttr(stmt, ODBC_v06.API.SQL_ATTR_ROW_ARRAY_SIZE, rowset, ODBC_v06.API.SQL_IS_UINTEGER)
    boundcols = Array{Any}(cols)
    indcols = Array{Vector{ODBC_v06.API.SQLLEN}}(cols)
    for x = 1:cols
        if longtexts[x]
            boundcols[x], indcols[x] = alloctypes[x][], ODBC_v06.API.SQLLEN[]
        else
            boundcols[x], elsize = internal_allocate(alloctypes[x], rowset, csizes[x])
            indcols[x] = Array{ODBC_v06.API.SQLLEN}(rowset)
            ODBC_v06.API.SQLBindCols(stmt, x, ODBC_v06.API.SQL2C[ctypes[x]], pointer(boundcols[x]), elsize, indcols[x])
        end
    end
    columns = ((allocate(T) for T in juliatypes)...,)
    schema = Data.Schema(juliatypes, cnames, rows >= 0 ? rows : Missings.missing,
        Dict("types"=>[ODBC_v06.API.SQL_TYPES[c] for c in ctypes], "sizes"=>csizes, "digits"=>cdigits, "nulls"=>cnulls))
    rowsfetched = Ref{ODBC_v06.API.SQLLEN}() # will be populated by call to SQLFetchScroll
    ODBC_v06.API.SQLSetStmtAttr(stmt, ODBC_v06.API.SQL_ATTR_ROWS_FETCHED_PTR, rowsfetched, ODBC_v06.API.SQL_NTS)
    types = [ODBC_v06.API.SQL2C[ctypes[x]] for x = 1:cols]
    source = ODBC_v06.Source(schema, dsn, query, columns, 100, rowsfetched, 0, boundcols, indcols, csizes, types, Type[longtexts[x] ? ODBC_v06.API.Long{T} : T for (x, T) in enumerate(juliatypes)], supportsreset == 1)
    rows != 0 && fetch!(source)
    return source
end

# primitive types
allocate(::Type{T}) where {T} = Vector{T}(0)
allocate(::Type{Union{Missing, WeakRefString{T}}}) where {T} = WeakRefStringArray(UInt8[], Union{Missing, WeakRefString{T}}, 0)

internal_allocate(::Type{T}, rowset, size) where {T} = Vector{T}(rowset), sizeof(T)
# string/binary types
internal_allocate(::Type{T}, rowset, size) where {T <: Union{UInt8, UInt16, UInt32}} = zeros(T, rowset * (size + 1)), sizeof(T) * (size + 1)

function fetch!(source)
    stmt = source.dsn.stmt_ptr
    source.status = ODBC_v06.API.SQLFetchScroll(stmt, ODBC_v06.API.SQL_FETCH_NEXT, 0)
    # source.rowsfetched[] == 0 && return
    # types = source.jltypes
    # for col = 1:length(types)
    #     ODBC_v06.cast!(types[col], source, col)
    # end
    return
end

# primitive types
function cast!(::Type{T}, source, col) where {T}
    len = source.rowsfetched[]
    c = source.columns[col]
    resize!(c, len)
    ind = source.indcols[col]
    data = source.boundcols[col]
    @simd for i = 1:len
        @inbounds c[i] = ifelse(ind[i] == ODBC_v06.API.SQL_NULL_DATA, missing, data[i])
    end
    return c
end


cast(::Type{Float64}, arr, cur, ind) = ind <= 0 ? DECZERO : parse(Float64, String(unsafe_wrap(Array, pointer(arr, cur), ind)))

function cast!(::Type{Union{Float64, Missing}}, source, col)
    len = source.rowsfetched[]
    c = source.columns[col]
    resize!(c, len)
    cur = 1
    elsize = source.sizes[col] + 1
    inds = source.indcols[col]
    @inbounds for i = 1:len
        ind = inds[i]
        c[i] = ind == ODBC_v06.API.SQL_NULL_DATA ? missing : cast(Float64, source.boundcols[col], cur, ind)
        cur += elsize
    end
    return c
end

cast(::Type{Vector{UInt8}}, arr, cur, ind) = arr[cur:(cur + max(ind, 0) - 1)]

function cast!(::Type{Union{Vector{UInt8}, Missing}}, source, col)
    len = source.rowsfetched[]
    c = source.columns[col]
    resize!(c, len)
    cur = 1
    elsize = source.sizes[col] + 1
    inds = source.indcols[col]
    @inbounds for i = 1:len
        ind = inds[i]
        c[i] = ind == ODBC_v06.API.SQL_NULL_DATA ? missing : cast(Vector{UInt8}, source.boundcols[col], cur, ind)
        cur += elsize
    end
    return c
end

# string types
bytes2codeunits(::Type{UInt8},  bytes) = ifelse(bytes == ODBC_v06.API.SQL_NULL_DATA, 0, Int(bytes))
bytes2codeunits(::Type{UInt16}, bytes) = ifelse(bytes == ODBC_v06.API.SQL_NULL_DATA, 0, Int(bytes >> 1))
bytes2codeunits(::Type{UInt32}, bytes) = ifelse(bytes == ODBC_v06.API.SQL_NULL_DATA, 0, Int(bytes >> 2))
codeunits2bytes(::Type{UInt8},  bytes) = ifelse(bytes == ODBC_v06.API.SQL_NULL_DATA, 0, Int(bytes))
codeunits2bytes(::Type{UInt16}, bytes) = ifelse(bytes == ODBC_v06.API.SQL_NULL_DATA, 0, Int(bytes * 2))
codeunits2bytes(::Type{UInt32}, bytes) = ifelse(bytes == ODBC_v06.API.SQL_NULL_DATA, 0, Int(bytes * 4))

function cast!(::Type{Union{String, Missing}}, source, col)
    len = source.rowsfetched[]
    c = source.columns[col]
    resize!(c, len)
    data = source.boundcols[col]
    T = eltype(data)
    cur = 1
    elsize = source.sizes[col] + 1
    inds = source.indcols[col]
    @inbounds for i in 1:len
        ind = inds[i]
        length = ODBC_v06.bytes2codeunits(T, max(ind, 0))
        c[i] = ind == ODBC_v06.API.SQL_NULL_DATA ? missing : (length == 0 ? "" : String(transcode(UInt8, data[cur:(cur + length - 1)])))
        cur += elsize
    end
    return c
end

function cast!(::Type{Union{WeakRefString{T}, Missing}}, source, col) where {T}
    len = source.rowsfetched[]
    c = source.columns[col]
    resize!(c, len)
    empty!(c.data)
    data = copy(source.boundcols[col])
    push!(c.data, data)
    cur = 1
    elsize = source.sizes[col] + 1
    inds = source.indcols[col]
    EMPTY = WeakRefString{T}(Ptr{T}(0), 0)
    @inbounds for i = 1:len
        ind = inds[i]
        length = ODBC_v06.bytes2codeunits(T, max(ind, 0))
        c[i] = ind == ODBC_v06.API.SQL_NULL_DATA ? missing : (length == 0 ? EMPTY : WeakRefString{T}(pointer(data, cur), length))
        cur += elsize
    end
    return c
end

# long types
const LONG_DATA_BUFFER_SIZE = 1024

function cast!(::Type{ODBC_v06.API.Long{Union{T, Missing}}}, source, col) where {T}
    stmt = source.dsn.stmt_ptr
    eT = eltype(source.boundcols[col])
    data = eT[]
    buf = zeros(eT, ODBC_v06.LONG_DATA_BUFFER_SIZE)
    ind = Ref{ODBC_v06.API.SQLLEN}()
    res = ODBC_v06.API.SQLGetData(stmt, col, source.ctypes[col], pointer(buf), sizeof(buf), ind)
    isnull = ind[] == ODBC_v06.API.SQL_NULL_DATA
    while !isnull
        len = ind[]
        oldlen = length(data)
        resize!(data, oldlen + bytes2codeunits(eT, len))
        ccall(:memcpy, Void, (Ptr{Void}, Ptr{Void}, Csize_t), pointer(data, oldlen + 1), pointer(buf), len)
        res = ODBC_v06.API.SQLGetData(stmt, col, source.ctypes[col], pointer(buf), length(buf), ind)
        res != ODBC_v06.API.SQL_SUCCESS && res != ODBC_v06.API.SQL_SUCCESS_WITH_INFO && break
    end
    c = source.columns[col]
    resize!(c, 1)
    c[1] = isnull ? missing : T(transcode(UInt8, data))
    return c
end

# DataStreams interface
Data.schema(source::ODBC_v06.Source) = source.schema
"Checks if an `ODBC_v06.Source` has finished fetching results from an executed query string"
Data.isdone(source::ODBC_v06.Source, x=1, y=1) = source.status != ODBC_v06.API.SQL_SUCCESS && source.status != ODBC_v06.API.SQL_SUCCESS_WITH_INFO
function Data.reset!(source::ODBC_v06.Source)
    source.supportsreset || throw(ArgumentError("Data.reset! not supported, probably due to the database vendor ODBC driver implementation"))
    stmt = source.dsn.stmt_ptr
    source.status = ODBC_v06.API.SQLFetchScroll(stmt, ODBC_v06.API.SQL_FETCH_FIRST, 0)
    source.rowoffset = 0
    return
end

Data.streamtype(::Type{ODBC_v06.Source}, ::Type{Data.Column}) = true
Data.streamtype(::Type{ODBC_v06.Source}, ::Type{Data.Field}) = true

function Data.streamfrom(source::ODBC_v06.Source, ::Type{Data.Field}, ::Type{Union{T, Missing}}, row, col) where {T}
    val = if isempty(source.columns[col])
        cast!(source.jltypes[col], source, col)[row - source.rowoffset]
    else
        source.columns[col][row - source.rowoffset]
    end

    if col == length(source.columns) && (row - source.rowoffset) == source.rowsfetched[] && !Data.isdone(source)
        ODBC_v06.fetch!(source)
        if source.rowsfetched[] > 0
            for i = 1:col
                cast!(source.jltypes[i], source, i)
            end
            source.rowoffset += source.rowsfetched[]
        end
    end
    return val
end

function Data.streamfrom(source::ODBC_v06.Source, ::Type{Data.Column}, ::Type{Union{T, Missing}}, row, col) where {T}
    dest = cast!(source.jltypes[col], source, col)
    if col == length(source.columns) && !Data.isdone(source)
        ODBC_v06.fetch!(source)
    end
    return dest
end

function query(dsn::DSN, sql::AbstractString, sink=DataFrame, args...; weakrefstrings::Bool=true, append::Bool=false, transforms::Dict=Dict{Int,Function}())
    sink = Data.stream!(Source(dsn, sql; weakrefstrings=weakrefstrings), sink, args...; append=append, transforms=transforms)
    return Data.close!(sink)
end

function query(dsn::DSN, sql::AbstractString, sink::T; weakrefstrings::Bool=true, append::Bool=false, transforms::Dict=Dict{Int,Function}()) where {T}
    sink = Data.stream!(Source(dsn, sql; weakrefstrings=weakrefstrings), sink; append=append, transforms=transforms)
    return Data.close!(sink)
end

query(source::ODBC_v06.Source, sink=DataFrame, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}()) = (sink = Data.stream!(source, sink, args...; append=append, transforms=transforms); return Data.close!(sink))
query(source::ODBC_v06.Source, sink::T; append::Bool=false, transforms::Dict=Dict{Int,Function}()) where {T} = (sink = Data.stream!(source, sink; append=append, transforms=transforms); return Data.close!(sink))

"Convenience string macro for executing an SQL statement against a DSN."
macro sql_str(s,dsn)
    query(dsn,s)
end
