using Plots

function plot_instance(depot, points; instance_name="instance")
    x_coords = map(point -> point[1], points)
    y_coords = map(point -> point[2], points)
    plot = scatter(x_coords, y_coords, markersize=4, legend=false)
    scatter!([depot[1]], [depot[2]], markersize=4, color="red")
    # path = "images/"
    # mkpath(path)
    # Plots.savefig(plot, path * "$instance_name.pdf")
end

function plot_path(depot, nodes, perm)
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
