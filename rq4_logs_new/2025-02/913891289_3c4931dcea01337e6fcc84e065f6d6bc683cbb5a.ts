"use client"

import { getItem } from "@/constants"
import TableService from "@/services/table-service"
import type TableDataModels from "@/types/tables"
import { useCallback, useEffect, useState } from "react"

const useTable = () => {
    const [table, setTable] = useState<TableDataModels[]>([])
    const [tableId_gbId, setTableId_gbId] = useState<string>('')
    const [currentOrderId_, setCurrentOrderId] = useState<string | null>(null)
    const [loading, setLoading] = useState<boolean>(false)
    const [error, setError] = useState<string | null>(null)



    const tableId = getItem("tableId")


    const fetchTableById = useCallback(async () => {
        if (!tableId) {
            setError("No table ID provided")
            return
        }

        setLoading(true)
        setError(null)
        try {
            const tableService = TableService.getInstance()
            const response = await tableService.getTableById(`${tableId}`)
            if (response.success && response.result) {
                setTable([response.result])
                setTableId_gbId(response.result.id)
                setCurrentOrderId(response.result.currentOrderId)
            } else {
                setTable([])
                setCurrentOrderId(null)
                setError(response.message || "No table data found")
            }
        } catch (err) {
            setError("Failed to fetch table")
            console.error(err)
        } finally {
            setLoading(false)
        }
    }, [tableId])

    useEffect(() => {
        fetchTableById()
    }, [fetchTableById])

    return { table, loading, error, currentOrderId_, tableId_gbId }
}

export default useTable
