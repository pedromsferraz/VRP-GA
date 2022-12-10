function read_instance(file_path)
    file = readchomp(file_path)
    lines = split(file, r"\n")
    Q = parse(Int64, lines[1])
    L = parse(Int64, lines[2])
    N = parse(Int64, lines[3])
    nodes = split.(lines[4:N+5], r" +")
    nodes = map(node -> parse.(Int64, node), nodes)

    return Q, L, N, nodes
end
