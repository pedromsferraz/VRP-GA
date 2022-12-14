using Distributions, LinearAlgebra

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

function instance_generator(N, K; W=1000, H=1000, sigma=100)
    x = rand(Uniform(0, W), K)
    y = rand(Uniform(0, H), K)
    depot = ceil.(Int64, [mean(x), mean(y)])
    centers = [[x[i], y[i]] for i in 1:K]
    covariance = diagm(repeat([sigma], 2))
    point_matrix = ceil.(Int64, hcat(map(mu -> rand(MvNormal(mu, covariance), N), centers)...))
    points = [point_matrix[:, j] for j in 1:size(point_matrix, 2)]
    println(depot[1], " ", depot[2])
    map(point -> println(point[1], ", ", point[2]), points)
    println(depot[1], " ", depot[2])
    return depot, points
end

function uniform_instance_generator(N; W=1000, H=1000, sigma=100)
    x = ceil.(Int64, rand(Uniform(0, W), N))
    y = ceil.(Int64, rand(Uniform(0, H), N))
    depot = ceil.(Int64, [mean(x), mean(y)])
    points = [[x[i], y[i]] for i in 1:N]
    println(depot[1], " ", depot[2])
    map(point -> println(point[1], ", ", point[2]), points)
    println(depot[1], " ", depot[2])
    return depot, points
end

function read_solution(solution_path)
    route_reg = r"\(List\(([0-9, ]*)\),List\(([0-9, ]*)\)\)"
    path_reg = r"\d, ArrayBuffer\(([-\d., ]*)\)"
    solution = readchomp(solution_path)
    lines = split(solution, r"\n")
    m = match(route_reg, lines[1])
    vehicle, path = map(s -> parse.(Int64, split(s, ", ")), m.captures)

    line_matches = map(line -> match(path_reg, line).captures, lines[2:end])
    fitness_values = map(s -> parse.(Float64, split(s[1], ", ")), line_matches)
    
    return (vehicle, path), fitness_values
end
