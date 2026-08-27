using FUSE
using IMAS
using Test
using ZMQ
using ProtoBuf

# Pull in the generated protobuf types via the same path the actor uses,
# so we encode/decode against the exact stub the actor was compiled against.
const _ZMQ_PB = include(joinpath(dirname(pathof(FUSE)), "actors", "zmq_proto_generated", "zmq_messages_pb.jl"))
using ._ZMQ_PB: FUSERequest, WireDataForFUSE, WireDataFromFUSE, Ack

# Tiny encode/decode helpers — mirror _pb_send/_pb_recv inside the actor.
_zmq_encode(msg) = (io = IOBuffer(); ProtoBuf.encode(ProtoBuf.ProtoEncoder(io), msg); take!(io))
_zmq_decode(::Type{T}, raw) where {T} = ProtoBuf.decode(ProtoBuf.ProtoDecoder(IOBuffer(raw)), T)

# Keyword-style constructor for WireDataForFUSE that survives future field additions:
# uses ProtoBuf.default_values (generated alongside the struct) for any field the
# caller doesn't override.
function _make_WireDataForFUSE(; kwargs...)
    defaults = ProtoBuf.default_values(WireDataForFUSE)
    overrides = NamedTuple(kwargs)
    vals = (haskey(overrides, f) ? overrides[f] : defaults[f] for f in keys(defaults))
    return WireDataForFUSE(vals...)
end

@testset "ActorZMQ round-trip" begin
    # ipc:// + tempname() avoids TCP port collisions on shared CI; Linux-only.
    endpoint = "ipc://" * tempname()

    # --- Round-trip happy path: metadata-only WireDataForFUSE (no psizr) ---
    # This exercises the wire format, the FUSERequest/WireDataForFUSE/Ack handshake,
    # the version handshake (FUSE.SCHEMA_VERSION echoed on both sides), the try/catch
    # wrappers, and the Bt-storage branch — without dragging in a fully-initialized
    # equilibrium (which would require a full FUSE.init).
    ini, act = FUSE.case_parameters(:ITER; init_from=:scalars)
    dd = IMAS.dd()
    act.ActorZMQ.enabled = true
    act.ActorZMQ.endpoint = endpoint
    act.ActorZMQ.timeout_ms = 5000  # short timeout so a bug fails the test instead of hanging on the default

    server = @async begin
        ctx = ZMQ.Context()
        sock = ZMQ.Socket(ctx, ZMQ.REP)
        ZMQ.bind(sock, endpoint)
        try
            req = _zmq_decode(FUSERequest, ZMQ.recv(sock))
            @test req.status == "ready"
            @test req.schema_version == FUSE.SCHEMA_VERSION
            resp = _make_WireDataForFUSE(;
                sim_time=0.1,
                Bt=2.1,
                has_Bt=true,
                schema_version=FUSE.SCHEMA_VERSION,
            )
            ZMQ.send(sock, _zmq_encode(resp))

            # Then a WireDataFromFUSE arrives; reply with an Ack with ok=true.
            _ = _zmq_decode(WireDataFromFUSE, ZMQ.recv(sock))
            ZMQ.send(sock, _zmq_encode(Ack("ack", true, "")))
        finally
            ZMQ.close(sock)
            ZMQ.close(ctx)
        end
    end

    actor = FUSE.ActorZMQ(dd, act)
    try
        FUSE.receive!(actor)
        @test !isempty(dd.equilibrium.vacuum_toroidal_field.b0)
        @test dd.equilibrium.vacuum_toroidal_field.b0[end] ≈ 2.1
        FUSE.send!(actor)
    finally
        FUSE.disconnect!(actor)
    end
    wait(server)
end

@testset "ActorZMQ rejects schema_version mismatch" begin
    # GSLite replies with schema_version=0 (pre-versioning binary). FUSE must
    # error loudly with both versions in the message and not proceed.
    endpoint = "ipc://" * tempname()
    ini, act = FUSE.case_parameters(:ITER; init_from=:scalars)
    dd = IMAS.dd()
    act.ActorZMQ.enabled = true
    act.ActorZMQ.endpoint = endpoint
    act.ActorZMQ.timeout_ms = 5000

    server = @async begin
        ctx = ZMQ.Context()
        sock = ZMQ.Socket(ctx, ZMQ.REP)
        ZMQ.bind(sock, endpoint)
        try
            _ = _zmq_decode(FUSERequest, ZMQ.recv(sock))
            resp = _make_WireDataForFUSE(; sim_time=0.0, schema_version=Int32(0))
            ZMQ.send(sock, _zmq_encode(resp))
        finally
            ZMQ.close(sock)
            ZMQ.close(ctx)
        end
    end

    actor = FUSE.ActorZMQ(dd, act)
    err = try
        FUSE.receive!(actor)
        nothing
    catch e
        e
    finally
        FUSE.disconnect!(actor)
    end
    wait(server)
    @test err !== nothing
    msg = sprint(showerror, err)
    @test occursin("schema mismatch", msg)
    @test occursin(string(FUSE.SCHEMA_VERSION), msg)
    @test occursin("version 0", msg)
end

@testset "ActorZMQ rejects ack.ok=false" begin
    # First exchange succeeds (so send! has somewhere to run); the second exchange
    # returns Ack(ok=false, error="solver diverged"). FUSE must surface the
    # rejection and tear down the coupled run.
    endpoint = "ipc://" * tempname()
    ini, act = FUSE.case_parameters(:ITER; init_from=:scalars)
    dd = IMAS.dd()
    act.ActorZMQ.enabled = true
    act.ActorZMQ.endpoint = endpoint
    act.ActorZMQ.timeout_ms = 5000

    server = @async begin
        ctx = ZMQ.Context()
        sock = ZMQ.Socket(ctx, ZMQ.REP)
        ZMQ.bind(sock, endpoint)
        try
            _ = _zmq_decode(FUSERequest, ZMQ.recv(sock))
            ZMQ.send(sock, _zmq_encode(_make_WireDataForFUSE(;
                sim_time=0.1,
                Bt=2.1,
                has_Bt=true,
                schema_version=FUSE.SCHEMA_VERSION,
            )))
            _ = _zmq_decode(WireDataFromFUSE, ZMQ.recv(sock))
            ZMQ.send(sock, _zmq_encode(Ack("nack", false, "solver diverged")))
        finally
            ZMQ.close(sock)
            ZMQ.close(ctx)
        end
    end

    actor = FUSE.ActorZMQ(dd, act)
    err = try
        FUSE.receive!(actor)
        FUSE.send!(actor)
        nothing
    catch e
        e
    finally
        FUSE.disconnect!(actor)
    end
    wait(server)
    @test err !== nothing
    msg = sprint(showerror, err)
    @test occursin("GSLite rejected", msg)
    @test occursin("solver diverged", msg)
end

@testset "ActorZMQ stores Ip_avg=0.0 when has_Ip_avg=true" begin
    # Regression for the proto3 zero-elision trap: a legitimate 0 A average during
    # ramp-up / pre-breakdown must land in aux[:zmq_Ip_avg] when GSLite advertises
    # presence with has_Ip_avg=true. The earlier `!= 0.0` check (since removed)
    # would have silently dropped this; the has_* bool is what the GSLite team
    # MUST set on the C++ side, and this test pins that contract.
    endpoint = "ipc://" * tempname()
    ini, act = FUSE.case_parameters(:ITER; init_from=:scalars)
    dd = IMAS.dd()
    act.ActorZMQ.enabled = true
    act.ActorZMQ.endpoint = endpoint
    act.ActorZMQ.timeout_ms = 5000

    server = @async begin
        ctx = ZMQ.Context()
        sock = ZMQ.Socket(ctx, ZMQ.REP)
        ZMQ.bind(sock, endpoint)
        try
            _ = _zmq_decode(FUSERequest, ZMQ.recv(sock))
            ZMQ.send(sock, _zmq_encode(_make_WireDataForFUSE(;
                sim_time=0.0,
                Ip_avg=0.0,
                has_Ip_avg=true,
                schema_version=FUSE.SCHEMA_VERSION,
            )))
        finally
            ZMQ.close(sock)
            ZMQ.close(ctx)
        end
    end

    actor = FUSE.ActorZMQ(dd, act)
    try
        FUSE.receive!(actor)
    finally
        FUSE.disconnect!(actor)
    end
    wait(server)

    aux = getfield(dd, :_aux)
    @test haskey(aux, :zmq_Ip_avg)
    @test aux[:zmq_Ip_avg].values == [0.0]
    @test aux[:zmq_Ip_avg].times == [dd.global_time]
end

@testset "ActorZMQ psizr is Z-fastest on the wire" begin
    # Regression for the silent-transpose bug. GSLite stores psizr(nz, nr) and flattens
    # it column-major, so the wire vector varies Z fastest. FUSE read it R-fastest; on
    # GSLite's square 33x33 grid both reshapes succeed and differ only by a transpose,
    # so nothing errored — the plasma just moved outside the first wall and crashed
    # FRESCO's axis search. A NON-SQUARE grid is used here on purpose: it makes the
    # wrong reading structurally impossible rather than merely wrong-valued.
    nR, nZ = 4, 7
    psi = [100.0 * i + k for i in 1:nR, k in 1:nZ]   # psi[iR, iZ], every entry distinct

    # how GSLite builds the wire vector: psizr(nz, nr), column-major
    wire = vec(permutedims(psi))
    @test length(wire) == nR * nZ
    @test wire[1:nZ] == psi[1, :]                    # Z is the fast index on the wire
    @test wire[nZ+1] == psi[2, 1]                    # ... and R only advances every nZ entries

    got = FUSE._psizr_to_matrix(wire, nR, nZ)
    @test size(got) == (nR, nZ)
    @test got == psi

    # the R-fastest reading FUSE used to do is a different matrix entirely
    @test reshape(wire, nR, nZ) != psi

    # a payload that fits neither grid is a hard error naming the actor and the sizes
    err = nothing
    try
        FUSE._psizr_to_matrix(wire, nR, nZ + 1)
    catch e
        err = e
    end
    @test err !== nothing
    @test occursin("psizr length", sprint(showerror, err))
end

@testset "ActorZMQ commits psizr on a non-square wire grid" begin
    # End-to-end through receive!: GSLite sends r_grid/z_grid with nR != nZ and a psi
    # whose extremum sits at a known, deliberately off-centre (R, Z). If the reshape
    # were transposed the committed map would not match psi[iR, iZ] — and with nR != nZ
    # it could not even be stored against the wire's own grid.
    endpoint = "ipc://" * tempname()
    ini, act = FUSE.case_parameters(:ITER; init_from=:scalars)
    dd = IMAS.dd()
    act.ActorZMQ.enabled = true
    act.ActorZMQ.endpoint = endpoint
    act.ActorZMQ.timeout_ms = 5000

    nR, nZ = 9, 13
    r_grid = collect(range(1.0, 2.6, length=nR))
    z_grid = collect(range(-1.5, 1.5, length=nZ))
    # elliptical well, minimum at (R=1.6, Z=+0.5): asymmetric in both axes, so a
    # transpose cannot reproduce it
    psi = [((r - 1.6) / 0.7)^2 + ((z - 0.5) / 1.1)^2 for r in r_grid, z in z_grid]

    # give receive! a slice to write into; no wall, so the boundary search may decline
    # to find a closed surface — the grid/psi commit is what this test pins.
    dd.global_time = 0.0
    dd.equilibrium.time = [0.0]
    resize!(dd.equilibrium.time_slice, 1)
    IMAS.retime!(dd.equilibrium.time_slice[1], 0.0)

    server = @async begin
        ctx = ZMQ.Context()
        sock = ZMQ.Socket(ctx, ZMQ.REP)
        ZMQ.bind(sock, endpoint)
        try
            _ = _zmq_decode(FUSERequest, ZMQ.recv(sock))
            ZMQ.send(sock, _zmq_encode(_make_WireDataForFUSE(;
                sim_time=0.0,
                schema_version=FUSE.SCHEMA_VERSION,
                psizr=vec(permutedims(psi)),      # Z fastest, as GSLite sends it
                r_grid=r_grid,
                z_grid=z_grid,
            )))
        finally
            ZMQ.close(sock)
            ZMQ.close(ctx)
        end
    end

    actor = FUSE.ActorZMQ(dd, act)
    try
        FUSE.receive!(actor)
    finally
        FUSE.disconnect!(actor)
    end
    wait(server)

    p2d = dd.equilibrium.time_slice[1].profiles_2d[1]
    @test length(p2d.grid.dim1) == nR
    @test length(p2d.grid.dim2) == nZ
    @test size(p2d.psi) == (nR, nZ)
    @test p2d.psi ≈ psi
    # the minimum lands where it was put, not at its transpose
    imin = argmin(p2d.psi)
    @test p2d.grid.dim1[imin[1]] ≈ 1.6
    @test p2d.grid.dim2[imin[2]] ≈ 0.5
end

# TODO: additional cases worth covering once the round-trip is stable:
#   - done=true short-circuit: receive! disconnects without populating dd
#   - psizr length vs nR*nZ mismatch: error message mentions "ActorZMQ" and "psizr"
#     (will need a fully-initialized dd to reach the equilibrium branch)
#   - had_psizr semantics: two successive receive!s, only the second carries psizr;
#     full flux_surfaces path runs on the second call, not the first
