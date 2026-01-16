import PropTypes from 'prop-types'
import { useDispatch } from 'react-redux'

import { clearSelectedFiles } from 'store/object'

import {
  downloadMultipleFiles,
  getPercentage,
  openSaveFileDialog,
} from 'lib/utils'

export const useDownloadObjects = ({ files = [], authTicket = '' }) => {
  const dispatch = useDispatch()

  const handleDownload = async () => {
    dispatch(clearSelectedFiles(false))

    const {
      addFileOperation,
      updateFileOperation,
      removeFileOperation,
      handleMultiDownload,
      addDownloadLoading,
    } = await import('store/object')

    if (files.length > 1) {
      dispatch(
        addFileOperation({
          operation: 'download',
          fileId: 'multi-download',
          status: 'loading',
          message: `Preparing ${files.length} files for download...`,
        })
      )
    } else if (files.length === 1) {
      const { name } = files[0]
      dispatch(
        addFileOperation({
          operation: 'download',
          fileName: name,
          fileId: 'single-download',
          status: 'loading',
          message: `Preparing ${name} for download...`,
        })
      )
    }

    // @ts-ignore
    window.handleDownloadProgress = (
      totalBytes,
      completedBytes,
      fileName,
      blobURL,
      error
    ) => {
      if (error) {
        console.error(error)
        return
      }

      const progress = getPercentage(completedBytes, totalBytes)
      const file = files.find(file => file.name === fileName)
      if (!file) return
      dispatch(
        addDownloadLoading({
          progress,
          fileId: file.lookup_hash,
          isDownloading: true,
        })
      )

      if (progress === 100 && blobURL) {
        setTimeout(() => {
          dispatch(
            addDownloadLoading({
              progress,
              fileId: file.lookup_hash,
              isDownloading: false,
            })
          )
        }, 2000)
      }
    }

    const fileParams = files.map(file => {
      const { path, lookup_hash, name } = file
      return {
        remotePath: path,
        localPath: '',
        downloadOp: 1, // 1 -> download file, 2 -> download thumbnail
        numBlocks: 100,
        remoteFileName: name,
        remoteLookupHash: lookup_hash,
      }
    })

    const downloads: string | any = await dispatch(
      handleMultiDownload({
        authTicket,
        files: JSON.stringify(fileParams),
        callbackFuncName: 'handleDownloadProgress',
      })
    )

    if (files.length > 1) {
      dispatch(
        updateFileOperation({
          fileId: 'multi-download',
          status: 'success',
          message: 'Download started...',
        })
      )

      setTimeout(() => dispatch(removeFileOperation('multi-download')), 2000)
    } else if (files.length === 1) {
      dispatch(
        updateFileOperation({
          fileId: 'single-download',
          status: 'success',
          message: 'Download started...',
        })
      )

      setTimeout(() => dispatch(removeFileOperation('single-download')), 2000)
    }

    if (downloads) {
      const downloadedFiles = JSON.parse(downloads)

      const allSuccess = downloadedFiles.every(
        file => file.commandSuccess === true
      )

      if (allSuccess) {
        downloadedFiles.length > 1
          ? await downloadMultipleFiles(downloadedFiles)
          : openSaveFileDialog(downloadedFiles[0])
      }
    }
  }

  return { handleDownload }
}

useDownloadObjects.propTypes = {
  files: PropTypes.array.isRequired,
  authTicket: PropTypes.string,
}