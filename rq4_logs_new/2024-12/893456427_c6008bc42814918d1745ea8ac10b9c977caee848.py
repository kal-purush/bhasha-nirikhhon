from src import (
    logger,
    parser,
    Path,
    Manager,
    Train
)

def main() -> None:
    logger.info("Start application")
    args = parser()

    if args.download:
        Manager.download_dataset(
            author="maysee",
            dataset_name="mushrooms-classification-common-genuss-images", 
            save_loaction=Path.datasets
        )

    if args.train:
        Train.execute(Path.mushrooms, Path.models)
    
    logger.info("Finish application")

if __name__ == "__main__":
    main()