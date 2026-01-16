from graphdiffusion.pipeline import *
import seaborn as sns


from graphdiffusion.plotting import plot_data_as_normal_pdf

# Generate the points
points = [torch.rand(2) for _ in range(1000)]

# Plotting
plt.clf()
plt.scatter([p[0].item() for p in points], [p[1].item() for p in points], alpha=0.6, s=50)
plt.title("Uniform random points in 2D")
plt.xlabel("X")
plt.ylabel("Y")
plt.axis("equal")
plt.savefig("random_points.png")


train_dataloader = DataLoader(points, batch_size=500, shuffle=True)
pipeline = PipelineVector(node_feature_dim=2, dist_type="L1")
pipeline.visualize_foward(
    data=train_dataloader,
    outfile="random_2d_forward.jpg",
    num=25,
)



train_dataloader = DataLoader(points, batch_size=50, shuffle=True)
pipeline = PipelineVector(node_feature_dim=2, dist_type="L1")
pipeline.visualize_foward(
    data=train_dataloader,
    outfile="random_2d_forward.jpg",
    plot_data_func=plot_data_as_normal_pdf,
    num=25,
)