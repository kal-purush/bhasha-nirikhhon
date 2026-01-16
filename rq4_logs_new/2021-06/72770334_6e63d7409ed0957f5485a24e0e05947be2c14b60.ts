import { ExperienceProps } from "../../../models/Data";
import { getExperienceProps } from "../../../utils/DataProvider";
import { Row, TableProps } from '../../../models/Table';

const mapExperienceToRow = (props: ExperienceProps): Row => {
    return {
        title: props.position,
        subtitle: props.company,
        detail: props.detail,
    };
};

export const getExperienceTableProps = (): TableProps => {
    const props = getExperienceProps();
    return { rows: Object.values(props.list).map((e) => mapExperienceToRow(e)) };
};