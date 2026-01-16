export type Loader = ({
  params,
}: {
  params: Record<string, string>
}) => Record<string, unknown> | Promise<Record<string, unknown>>