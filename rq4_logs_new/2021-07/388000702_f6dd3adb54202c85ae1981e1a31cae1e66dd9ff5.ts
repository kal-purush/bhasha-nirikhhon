import * as yup from 'yup';
import type { SchemaOf } from 'yup';
import gql from 'graphql-tag';
import { useMutation, useQuery } from 'urql';
import { IQuery, IMutation } from '../../schema';

interface FakeType {
    __typename: string;
    name: string;
}

type IObjectType = Omit<FakeType, '__typename'>;

export const schema: SchemaOf<IObjectType> = yup.object().shape({
    name: yup.string().required(),
});

export const initialValues: IObjectType = {
    name: '',
};