using Plots

function plot_instance(points; instance_name="instance")
    x_coords = map(point -> point[1], points)
    y_coords = map(point -> point[2], points)
    plot = Plots.scatter(x_coords, y_coords, markersize=4, legend=false)
    # path = "images/"
    # mkpath(path)
    # Plots.savefig(plot, path * "$instance_name.pdf")
end

function plot_path(nodes, perm)
    N = length(perm)
    fig = plot_instance(nodes)
    path = nodes[perm]
    for i in 2:N
        plot!([path[i-1][1], path[i][1]], [path[i-1][2], path[i][2]], color="black", arrow=true)
    end
    plot!([path[N][1], path[1][1]], [path[N][2], path[1][2]], color="black", arrow=true)
    return fig
end
