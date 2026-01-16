import PropTypes from 'prop-types'
import { useDispatch } from 'react-redux'

export const useDeleteObjects = ({ files, close }) => {
  const dispatch = useDispatch()

  const fileParams = files.map(file => {
    return {
      operationType: 'delete',
      remotePath: file.path,
    }
  })

  const handleDelete = async () => {
    close()

    const {
      addFileOperation,
      clearSelectedFiles,
      removeFileOperation,
      // addEffectedFile,
      updateFileOperation,
      handleMultiOperation,
    } = await import('store/object')

    dispatch(clearSelectedFiles(false))

    if (files.length > 1) {
      dispatch(
        addFileOperation({
          operation: 'delete',
          fileId: 'multi-delete',
          status: 'loading',
          message: `Deleting ${files.length} files...`,
        })
      )

      // files.forEach(file => dispatch(addEffectedFile(file.lookup_hash)))
    } else if (files.length === 1) {
      const { name } = files[0]

      dispatch(
        addFileOperation({
          operation: 'delete',
          fileName: name,
          fileId: 'single-delete',
          status: 'loading',
          message: `Deleting ${name}...`,
        })
      )

      // dispatch(addEffectedFile(lookup_hash))
    }

    try {
      await dispatch(handleMultiOperation(JSON.stringify(fileParams)))

      if (files.length > 1) {
        dispatch(
          updateFileOperation({
            operation: 'delete',
            fileId: 'multi-delete',
            status: 'success',
            message: `Deleted ${files.length} files successfully`,
          })
        )

        setTimeout(() => dispatch(removeFileOperation('multi-delete')), 2000)
      } else if (files.length === 1) {
        dispatch(
          updateFileOperation({
            operation: 'delete',
            fileId: 'single-delete',
            status: 'success',
            message: `Deleted ${files[0]?.name} successfully`,
          })
        )

        setTimeout(() => dispatch(removeFileOperation('single-delete')), 2000)
      }
    } catch (err) {
      console.error(err)

      if (files.length > 1) {
        dispatch(
          updateFileOperation({
            operation: 'delete',
            fileId: 'multi-delete',
            status: 'error',
            message: `Failed to deleted ${files.length} files`,
          })
        )

        setTimeout(() => dispatch(removeFileOperation('multi-delete')), 2000)
      } else if (files.length === 1) {
        dispatch(
          updateFileOperation({
            operation: 'delete',
            fileId: 'single-delete',
            status: 'error',
            message: `Failed to Deleted ${files[0]?.name}`,
          })
        )

        setTimeout(() => dispatch(removeFileOperation('single-delete')), 2000)
      }
    }
  }

  return { handleDelete }
}

useDeleteObjects.propTypes = {
  files: PropTypes.array.isRequired,
  close: PropTypes.func.isRequired,
}