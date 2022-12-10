using Random, StatsBase
include("utils.jl")
include("plots.jl")

# fitness function (inverse of path length)
distance(i1, i2) = sqrt((i1[1] - i2[1])^2 + (i1[2] - i2[2])^2)
function fitness(nodes, perm)
    N = length(perm)
    sum_dist = 0
    for i in 2:N
        sum_dist += distance(nodes[perm[i-1]], nodes[perm[i]])
    end
    sum_dist += distance(nodes[perm[N]], nodes[perm[1]])
    return -sum_dist
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

# crossover of paths p1 and p2
function crossover(p1, p2)
    pos1 = rand(1:length(p1))
    pos2 = rand(1:length(p1))
    if pos1 > pos2
        pos1, pos2 = pos2, pos1
    end

    childP1 = p1[pos1:pos2]
    childP2 = [item for item in p2 if !(item in childP1)]
    return vcat(childP2[1:pos1-1], childP1, childP2[pos1:end])
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

# randomly mutates permutations to increase variability
function mutate(population, elite_size; mutation_probability=0.01, mutation_probability_adjacent=0.01)
    new_population = population[1:elite_size]
    for p in population[elite_size+1:end]
        if rand() < mutation_probability
            pos2 = rand(1:length(p))
            pos1 = (pos2 == 1) ? 2 : pos2-1
            p[pos1], p[pos2] = p[pos2], p[pos1]
        end
        if rand() < mutation_probability_adjacent
            pos2 = rand(1:length(p))
            pos1 = rand(1:length(p))
            p[pos1], p[pos2] = p[pos2], p[pos1]
        end
        push!(new_population, p)
    end
    return new_population
end

# GA algorithm
function run_ga(nodes, population_size, elite_size; crossover_probability=0.75, mutation_probability=0.001, mutation_probability_adjacent=0.005, n_iter=100)
    N = length(nodes)
    population = [randperm(N) for _ in 1:population_size]
    fitness_values = []
    for t in 1:n_iter
        population_fitness = [fitness(nodes, population[i]) for i in 1:population_size]
        selected_population = tournment_selection(population, population_fitness, elite_size, population_size-elite_size, K=5)
        offspring = breed(selected_population, elite_size; crossover_probability=crossover_probability)
        population = mutate(offspring, elite_size, mutation_probability=mutation_probability, mutation_probability_adjacent=mutation_probability_adjacent)
        push!(fitness_values, maximum(population_fitness))
    end
    population_fitness = [fitness(nodes, population[i]) for i in 1:population_size]
    sort_order = sortperm(population_fitness, rev=true)
    best = population[sort_order[1]]
    return best, population_fitness[sort_order[1]], fitness_values
end

dataset_path = (@__DIR__) * "/mpdpsl_instances/Small/"
instance_name = "mpdpsl10-1"
file_name = dataset_path * instance_name * ".txt"

# Q (capacity), L (maximum route length), N (number of locations), nodes
Q, L, N, nodes = read_instance(file_name)
plot_instance(nodes[1:end])

best, best_fit, fitness_values = run_ga(nodes, 10000, 100, n_iter=100, crossover_probability=0.95, mutation_probability=0.0, mutation_probability_adjacent=0.01)
fig = plot_path(nodes, best)
best_fit

plot(-fitness_values, label=false, title="Total distance")

savefig(fig, "images/$instance_name.pdf")
