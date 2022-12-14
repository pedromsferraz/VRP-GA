using Random, StatsBase
include("utils.jl")
include("plots.jl")

function fix_partition(p, N)
    p_size = length(p)
    diff = sign(N - sum(p))
    while (sum(p) != N)
        pos = rand(1:p_size)
        if diff < 0 && p[pos] == 0
            continue
        end
        p[pos] += sign(diff)
    end
    return p
end

# fitness function (inverse of path length)
distance(i1, i2) = sqrt((i1[1] - i2[1])^2 + (i1[2] - i2[2])^2)
function compute_path_length(depot, path)
    N = length(path)
    if length(path) == 0
        return 0
    end
    sum_dist = distance(depot, path[1])
    for i in 2:N
        sum_dist += distance(path[i-1], path[i])
    end
    sum_dist += distance(path[N], depot)
    return sum_dist
end

function fitness(depot, nodes, route)
    vehicles, tour = route
    N = length(tour)
    
    paths = []
    cur = 1
    for v in vehicles
        push!(paths, tour[cur:cur+v-1])
        cur = cur+v
    end
    costs = map(path -> compute_path_length(depot, nodes[path]), paths)
    
    return -maximum(costs)
end

# selects individuals for breeding
# probability of selection proportional to fitness (doesn't work with negative fitness values)
# best individuals are passed by elitism
function roulette_selection(population, population_fitness, elite_size, random_selection_size)
    sort_order = sortperm(population_fitness, rev=true)
    ranges = cumsum(population_fitness) / sum(population_fitness)

    elite_pop = population[sort_order[1:elite_size]]

    v = rand(random_selection_size)
    pos = [findfirst(val -> val > v[i], ranges) for i in 1:random_selection_size]
    random_pop = population[pos]

    return vcat(elite_pop, random_pop)
end

# selects individuals for breeding
# for each new individual selects best out of K samples
# best individuals are passed by elitism
function tournment_selection(population, population_fitness, elite_size, random_selection_size; K=5)
    N = length(population)
    sort_order = sortperm(population_fitness, rev=true)
    elite_pop = population[sort_order[1:elite_size]]

    random_pop = []
    for _ in 1:random_selection_size
        individuals = sample(1:N, K, replace=false)
        max_index = argmax(population_fitness[individuals])
        best_K = population[individuals[max_index]]
        push!(random_pop, best_K)
    end
    return vcat(elite_pop, random_pop)
end

# crossover of vehicles of p1 and p2
function uniform_crossover(v1, v2, N)
    size = length(v1)
    flips = rand([true, false], size)
    child = [flips[i] ? v1[i] : v2[i] for i in 1:size]
    child = fix_partition(child, N)
    return child
end

# crossover of paths of p1 and p2
function order_crossover(p1, p2)
    pos1 = rand(1:length(p1))
    pos2 = rand(1:length(p1))
    if pos1 > pos2
        pos1, pos2 = pos2, pos1
    end

    childP1 = p1[pos1:pos2]
    childP2 = [item for item in p2 if !(item in childP1)]
    return vcat(childP2[1:pos1-1], childP1, childP2[pos1:end])
end

# crossover of individuals p1 and p2
function crossover(i1, i2)
    N = length(i1[2])
    return (uniform_crossover(i1[1], i2[1], N), order_crossover(i1[2], i2[2]))
end

# performs crossover to create new paths
function breed(selected_population, elite_size; crossover_probability=0.75)    
    elite_children = selected_population[1:elite_size]

    offspring_children = []
    offspring_size = length(selected_population) - elite_size    
    for i in 1:offspring_size
        if rand() < crossover_probability
            p1 = sample(selected_population)
            p2 = sample(selected_population)
            child = crossover(p1, p2)
        else
            child = sample(selected_population)
        end
        push!(offspring_children, child)
    end

    return vcat(elite_children, offspring_children)
end

function mutate_vehicle(v, N, mutation_probability)
    if rand() < mutation_probability
        v[rand(1:length(v))] = rand(0:maximum(v))
        v = fix_partition(v, N)
    end
    return v
end

function mutate_path(p, mutation_probability, mutation_probability_adjacent)
    if rand() < mutation_probability_adjacent
        pos2 = rand(1:length(p))
        pos1 = (pos2 == 1) ? 2 : pos2-1
        p[pos1], p[pos2] = p[pos2], p[pos1]
    end
    if rand() < mutation_probability
        pos2 = rand(1:length(p))
        pos1 = rand(1:length(p))
        p[pos1], p[pos2] = p[pos2], p[pos1]
    end
    return p
end

# randomly mutates permutations to increase variability
function mutate(population, elite_size; mutation_probability=0.01, mutation_probability_adjacent=0.01, uniform_mutation_probability=0.01)
    new_population = population[1:elite_size]
    for ind in population[elite_size+1:end]
        N = length(ind[2])
        ind1 = mutate_vehicle(ind[1], N, uniform_mutation_probability)
        ind2 = mutate_path(ind[2], mutation_probability, mutation_probability_adjacent)
        ind = (ind1, ind2)
        push!(new_population, ind)
    end
    return new_population
end

# GA algorithm
function run_ga(depot, nodes, population_size, elite_size, n_vehicles; crossover_probability=0.75, mutation_probability=0.001, mutation_probability_adjacent=0.005, uniform_mutation_probability=0.005, n_iter=100)
    N = length(nodes)
    part = zeros(Int64, n_vehicles)
    population = [(fix_partition(part, N), randperm(N)) for _ in 1:population_size]
    fitness_values = []
    for t in 1:n_iter
        population_fitness = [fitness(depot, nodes, population[i]) for i in 1:population_size]
        selected_population = tournment_selection(population, population_fitness, elite_size, population_size-elite_size, K=5)
        offspring = breed(selected_population, elite_size; crossover_probability=crossover_probability)
        population = mutate(offspring, elite_size, mutation_probability=mutation_probability, mutation_probability_adjacent=mutation_probability_adjacent, uniform_mutation_probability=uniform_mutation_probability)
        push!(fitness_values, maximum(population_fitness))
    end
    population_fitness = [fitness(depot, nodes, population[i]) for i in 1:population_size]
    sort_order = sortperm(population_fitness, rev=true)
    best = population[sort_order[1]]
    return best, population_fitness[sort_order[1]], fitness_values
end

dataset_path = (@__DIR__) * "/mpdpsl_instances/Small/"
instance_name = "mpdpsl10-1"
file_name = dataset_path * instance_name * ".txt"

# Q (capacity), L (maximum route length), N (number of locations), nodes
Q, L, N, nodes = read_instance(file_name)

depot = nodes[1]
nodes = nodes[2:end-1]
plot_instance(depot, nodes[1:end])

best, best_fit, fitness_values = run_ga(depot, nodes, 10000, 100, 3, n_iter=300, crossover_probability=0.95, mutation_probability=0.01, mutation_probability_adjacent=0.05, uniform_mutation_probability=0.05)
fig = plot_tsp_path(depot, nodes, best[2])
fig = plot_vrp_path(depot, nodes, best)
best_fit

plot(-fitness_values, label=false, title="Total distance")

savefig(fig, "images/$instance_name.pdf")

depot, nodes = instance_generator(25, 3, sigma=4000)
plot_instance(depot, nodes)
best, best_fit, fitness_values = run_ga(depot, nodes, 5000, 10, 5, n_iter=1000, crossover_probability=0.95, mutation_probability=0.05, mutation_probability_adjacent=0.05, uniform_mutation_probability=0.05)
fig = plot_vrp_path(depot, nodes, best)

depot, nodes = uniform_instance_generator(100)
plot_instance(depot, nodes)

instance_name = "uniform-1"
Q, L, N, nodes = read_instance(dataset_path * instance_name * ".txt")
depot = nodes[1]
nodes = nodes[2:end-1]
plot_instance(depot, nodes)

best, fitness_values = read_solution("julia/solution-$instance_name.txt")
fig = plot_vrp_path(depot, nodes, best)
fig = plot(-fitness_values, label=false, title="Path cost")
Plots.savefig(fig, "images/fitness_$instance_name.pdf")
