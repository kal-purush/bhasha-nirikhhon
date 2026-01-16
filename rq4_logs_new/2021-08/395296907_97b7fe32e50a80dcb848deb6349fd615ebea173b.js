import CheckIcon from '@material-ui/icons/Check';

export default function PowerGridFormattedCell(props) {
    const { data } = props
    if (data === "TRUE") {
        return (
            <CheckIcon className="text-success" />
        )
    }

    return (
        <i></i>
    )
}