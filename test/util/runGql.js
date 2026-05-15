import { graphql } from 'graphql'

export async function runGql({ schema, source, variableValues, contextValue }) {
  const result = await graphql({ schema, source, variableValues, contextValue })
  // GraphQL returns data objects with null prototypes; normalizing keeps deepStrictEqual happy.
  const data = result.data == null ? result.data : JSON.parse(JSON.stringify(result.data))
  return { ...result, data }
}
