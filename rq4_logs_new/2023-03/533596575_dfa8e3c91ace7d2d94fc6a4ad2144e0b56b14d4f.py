import hydra
from hydra.core.config_store import ConfigStore
from hydra_zen import make_config
from hydra_zen import make_custom_builds_fn
from hydra_zen import save_as_yaml
from omegaconf import DictConfig
from omegaconf import OmegaConf

from pyrovelocity.api import train_model
from pyrovelocity.config import print_config_tree
from pyrovelocity.utils import get_pylogger


# define helper functions
pbuilds = make_custom_builds_fn(zen_partial=True, populate_full_signature=True)


def create_dataset_config(name, dl_root, data_file, rel_path, url, process_method):
    return dict(
        data_file=data_file,
        dl_root=dl_root,
        dl_path="${.dl_root}/${.data_file}",
        rel_path=rel_path,
        url=url,
        derived=dict(
            process_method=process_method,
            rel_path="${data_external.processed_path}/" + f"{name}_processed.h5ad",
        ),
    )


def create_model_config(
    source, name, model_suffix, vector_field_basis, **custom_training_parameters
):
    return dict(
        path="${paths.models}/" + f"{name}_model{model_suffix}",
        model_path="${.path}/model",
        input_data_path="${data_external." + f"{source}.{name}" + ".derived.rel_path}",
        trained_data_path="${.path}/trained.h5ad",
        pyrovelocity_data_path="${.path}/pyrovelocity.pkl",
        metrics_path="${.path}/metrics.json",
        run_info_path="${.path}/run_info.json",
        vector_field_parameters=dict(basis=vector_field_basis),
        training_parameters=pbuilds(
            train_model,
            loss_plot_path="${..path}/loss_plot.png",
            patient_improve=0.0001,
            patient_init=45,
            max_epochs=4000,
            **custom_training_parameters,
        ),
    )


def create_reports_config(model_name: str, model_number: int):
    path = f"{paths['reports']}/{model_name}_model{model_number}"
    return dict(
        path=path,
        shared_time_plot=f"{path}/shared_time.pdf",
        volcano_plot=f"{path}/volcano.pdf",
        rainbow_plot=f"{path}/rainbow.pdf",
        uncertainty_param_plot=f"{path}/param_uncertainties.pdf",
        vector_field_plot=f"{path}/vector_field.pdf",
        biomarker_selection_plot=f"{path}/markers_selection_scatterplot.tif",
        biomarker_phaseportrait_plot=f"{path}/markers_phaseportrait.pdf",
    )


# define configuration
base = dict(log_level="INFO")

paths = dict(
    data="data",
    models="models",
    reports="reports",
)

config = make_config(
    base=base,
    paths=paths,
    data_external=dict(
        root_path="${paths.data}/external",
        processed_path="${paths.data}/processed",
        sources=["velocyto", "scvelo", "pyrovelocity"],
        velocyto=dict(
            download=["pons"],
            process=["pons"],
            sources=dict(
                gcs_root_url="https://storage.googleapis.com/pyrovelocity/data"
            ),
            pons=create_dataset_config(
                "pons",
                dl_root="${data_external.root_path}",
                data_file="oligo_lite.h5ad",
                rel_path="${data_external.root_path}/oligo_lite.h5ad",
                url="${data_external.velocyto.sources.gcs_root_url}/oligo_lite.h5ad",
                process_method="load_data",
            ),
        ),
        scvelo=dict(
            download=["pancreas", "pbmc68k"],
            process=["pancreas", "pbmc68k"],
            sources=dict(
                figshare_root_url="https://ndownloader.figshare.com/files",
                scvelo_root_url="https://github.com/theislab/scvelo_notebooks/raw/master",
            ),
            pancreas=create_dataset_config(
                "pancreas",
                dl_root="data/Pancreas",
                data_file="endocrinogenesis_day15.h5ad",
                rel_path="${data_external.root_path}/endocrinogenesis_day15.h5ad",
                url="${data_external.scvelo.sources.scvelo_root_url}/data/Pancreas/endocrinogenesis_day15.h5ad",
                process_method="load_data",
            ),
            pbmc68k=create_dataset_config(
                "pbmc68k",
                dl_root="data/PBMC",
                data_file="pbmc68k.h5ad",
                rel_path="${data_external.root_path}/pbmc68k.h5ad",
                url="${data_external.scvelo.sources.figshare_root_url}/27686886",
                process_method="load_pbmc",
            ),
        ),
        pyrovelocity=dict(
            download=["larry"],
            process=[],
            sources=dict(figshare_root_url="https://ndownloader.figshare.com/files"),
            larry=create_dataset_config(
                "larry",
                dl_root="${data_external.root_path}",
                data_file="larry.h5ad",
                rel_path="${data_external.root_path}/larry.h5ad",
                url="${data_external.pyrovelocity.sources.figshare_root_url}/37028569",
                process_method="",
            ),
        ),
    ),
    model_training=dict(
        train=[
            "pancreas_model1",
            "pancreas_model2",
            "pbmc68k_model1",
            "pbmc68k_model2",
            "pons_model1",
            "pons_model2",
        ],
        pancreas_model1=create_model_config(
            "scvelo", "pancreas", 1, "umap", guide_type="auto_t0_constraint"
        ),
        pancreas_model2=create_model_config(
            "scvelo", "pancreas", 2, "umap", offset=True
        ),
        pbmc68k_model1=create_model_config(
            "scvelo",
            "pbmc68k",
            1,
            "tsne",
            guide_type="auto_t0_constraint",
            cell_state="celltype",
        ),
        pbmc68k_model2=create_model_config(
            "scvelo", "pbmc68k", 2, "tsne", cell_state="celltype", offset=True
        ),
        pons_model1=create_model_config(
            "velocyto",
            "pons",
            1,
            "umap",
            guide_type="auto_t0_constraint",
            cell_state="celltype",
        ),
        pons_model2=create_model_config(
            "velocyto", "pons", 2, "umap", cell_state="celltype", offset=True
        ),
    ),
    reports=dict(
        model_summary=dict(
            summarize=[
                "pancreas_model1",
                "pancreas_model2",
                "pbmc68k_model1",
                "pbmc68k_model2",
                "pons_model1",
                "pons_model2",
            ],
            pancreas_model1=create_reports_config("pancreas", 1),
            pancreas_model2=create_reports_config("pancreas", 2),
            pbmc68k_model1=create_reports_config("pbmc68k", 1),
            pbmc68k_model2=create_reports_config("pbmc68k", 2),
            pons_model1=create_reports_config("pons", 1),
            pons_model2=create_reports_config("pons", 2),
        ),
        figure2=dict(
            tag="fig2",
            path="${paths.reports}/${.tag}",
            tif_path="${.path}/${.tag}_raw_gene_selection_model1.tif",
            svg_path="${.path}/${.tag}_raw_gene_selection_model1.svg",
            biomarker_selection_path="${.path}/${.tag}_markers_selection_scatterplot.tif",
            biomarker_phaseportrait_path="${.path}/${.tag}_markers_phaseportrait.pdf",
            uncertainty_param_plot_path="${.path}/${.tag}_param_uncertainties.pdf",
            uncertainty_magnitude_plot_path="${.path}/${.tag}_magnitude_uncertainties.pdf",
            pancreas_model1=dict(
                shared_time_plot="${..path}/${..tag}_pancreas_shared_time.pdf",
                volcano_plot="${..path}/${..tag}_pancreas_volcano.pdf",
                rainbow_plot="${..path}/${..tag}_pancreas_rainbow.pdf",
                vector_field_plot="${..path}/${..tag}_pancreas_vector_field.pdf",
            ),
        ),
        figureS3=dict(
            tag="figS3",
            path="${paths.reports}/${.tag}",
            tif_path="${.path}/${.tag}_raw_gene_selection_model2.tif",
            svg_path="${.path}/${.tag}_raw_gene_selection_model2.svg",
        ),
    ),
)

# register the configuration in hydra's ConfigStore
cs = ConfigStore.instance()
cs.store(name="config", node=config)


@hydra.main(version_base="1.2", config_name="config")
def main(conf: DictConfig) -> None:
    """Generate pipeline configuration.
    Args:
        conf {DictConfig}: OmegaConf configuration for hydra
    """
    logger = get_pylogger(name="CONF", log_level="INFO")
    logger.info(f"\n\nResolving global configuration file\n\n")
    OmegaConf.resolve(conf)
    print_config_tree(conf, logger, ())
    save_as_yaml(conf, "zenconfig.yaml")


if __name__ == "__main__":
    main()