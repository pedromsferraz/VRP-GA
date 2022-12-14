using Plots

function plot_instance(depot, points; instance_name="instance")
    x_coords = map(point -> point[1], points)
    y_coords = map(point -> point[2], points)
    plot = scatter(x_coords, y_coords, markersize=4, legend=false)
    scatter!([depot[1]], [depot[2]], markersize=4, color="red")
    # path = "images/"
    # mkpath(path)
    # Plots.savefig(plot, path * "$instance_name.pdf")
    return plot
end

function plot_tsp_path(depot, nodes, perm)
    N = length(perm)
    fig = plot_instance(depot, nodes)
    path = nodes[perm]
    plot!([depot[1], path[1][1]], [depot[2], path[1][2]], color="black", arrow=true)
    for i in 2:N
        plot!([path[i-1][1], path[i][1]], [path[i-1][2], path[i][2]], color="black", arrow=true)
    end
    plot!([path[N][1], depot[1]], [path[N][2], depot[2]], color="black", arrow=true)
    return fig
end

function plot_path(depot, nodes, perm; color="black")
    N = length(perm)
    path = nodes[perm]
    if length(perm) == 0
        return
    end
    fig = plot!([depot[1], path[1][1]], [depot[2], path[1][2]], color=color, arrow=true)
    for i in 2:N
        fig = plot!([path[i-1][1], path[i][1]], [path[i-1][2], path[i][2]], color=color, arrow=true)
    end
    fig = plot!([path[N][1], depot[1]], [path[N][2], depot[2]], color=color, arrow=true)
    return fig
end

function plot_vrp_path(depot, nodes, route)
    fig = plot_instance(depot, nodes)
    vehicles, tour = route

    paths = []
    cur = 1
    for v in vehicles
        fig = push!(paths, tour[cur:cur+v-1])
        cur = cur+v
    end

    for (i, path) in enumerate(paths)
        fig = plot_path(depot, nodes, path; color=i)
    end

    path = "images/"
    mkpath(path)
    Plots.savefig(fig, path * "$instance_name.pdf")

    return fig
end
