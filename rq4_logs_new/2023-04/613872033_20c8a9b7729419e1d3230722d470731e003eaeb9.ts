import { useAxios } from "@vueuse/integrations/useAxios"
import { withDirectives } from "vue"
import type { Resp } from "src/server/utils/http"
import type { TreeDropInfo, TreeOption } from "naive-ui/es/tree/src/interface"
import type { FileItem } from "src/server/service"
import type { onUpdateExpandedKeys } from "naive-ui/es/tree/src/Tree"
import client from "./client"

function listTemplates() {
  return useAxios<Resp<FileItem[]>>("/template", client)
}

function addTemplate(name: string, content?: string) {
  return useAxios<Resp<unknown>>("/template/add", { method: "POST", data: { name, content: content || "" } }, client)
}

export function saveTemplate(path: string, content?: string, update = false) {
  return useAxios<Resp<unknown>>("/template/save", { method: "POST", data: { path, content: content || "", update } }, client)
}

function trashTemplate(name: string, physics?: boolean) {
  return useAxios<Resp<unknown>>("/template/trash", { method: "DELETE", data: { name, physics } }, client)
}

function loadTemplate(path: string) {
  return useAxios<Resp<string>>("/template/get", { method: "GET", params: { path } }, client)
}

export const useTemplateTree = () => {
  const data = ref<TreeOption[]>([])
  const searchKey = ref<string>()
  const selectedKey = ref<string>()
  const { data: dataList, execute: list, isFinished } = listTemplates()
  const fileIconNode = h("div", { class: "i-simple-icons-handlebarsdotjs" })
  const dirIconNode = h("div", { class: "i-mdi-folder" })

  watch(isFinished, () => {
    if (isFinished.value) {
      const toTreeOption = (i: FileItem): TreeOption => ({
        key: i.path,
        value: i.name,
        prefix: () => i.children?.length ? dirIconNode : fileIconNode,
        isLeaf: !i.children,
        children: i.children?.map(toTreeOption),
      })
      data.value = dataList.value?.data?.map<TreeOption>(toTreeOption) || []
    }
  })

  function removeVirtual() {
    data.value = data.value.filter(i => !i.isVirtual) || []
  }

  function newFile() {
    const virtualItem = document.querySelector<HTMLDivElement>(".virtual-item")
    if (virtualItem) {
      virtualItem.focus()
      return
    } else {
      addVirtual()
    }

    nextTick(() => {
      const virtualItem = document.querySelector<HTMLDivElement>(".virtual-item")

      const handler = (e: KeyboardEvent) => {
        if (e.key === "Enter") {
          removeVirtual()
          if (virtualItem?.innerText)addFile(virtualItem?.innerText)
          window?.removeEventListener("keydown", handler)
        } else if (e.key === "Escape") {
          clear()
        }
      }
      window?.addEventListener("keydown", handler)

      const clear = () => {
        removeVirtual()
        stop?.()
        window?.removeEventListener("keydown", handler)
      }
      const stop = onClickOutside(virtualItem, clear)
    })
  }

  async function getTemplate(path: string) {
    return loadTemplate(path)
  }

  function addFile(name: string) {
    addTemplate(name)
    list()
  }

  async function trashFile(key?: string) {
    if (!key) return
    // for (const key of keys) {
    //   const name = data.value?.[key].value as string
    //   trashTemplate(name.slice(0, name.length - 4))
    // }
    trashTemplate(key, true)
    setTimeout(() => {
      list()
    })
  }

  function refresh() {
    searchKey.value = undefined
    list()
  }

  function save(path: string, content: string, update: boolean) {
    saveTemplate(path, content, update)
  }

  function addVirtual() {
    data.value?.push({
      key: data.value?.length || 0,
      isVirtual: true,
      prefix: () => fileIconNode,
      value: () => withDirectives(h("div", { class: "virtual-item", contenteditable: "true" }), [[vFocus]]),
    })
  }

  function newFolder() {
    //
  }

  // @ts-expect-error type use undefined instead of null
  const onSelect: OnUpdateSelectedKeys = (
    _keys: Array<string | number>,
    _option: Array<TreeOption | undefined>,
    meta: {
      node: TreeOption | undefined
      action: "select" | "unselect"
    }) => {
    if (!meta.node) return
    if (meta.action === "select") {
      selectedKey.value = meta.node.key as string
    }
  }


  // @ts-expect-error type use undefined instead of null
  const animationOnExpand: onUpdateExpandedKeys = (
    _keys: Array<string | number>,
    _option: Array<TreeOption | undefined>,
    meta: {
      node: TreeOption | undefined
      action: "expand" | "collapse" | "filter"
    },
  ) => {
    if (!meta.node) return
    switch (meta.action) {
      case "expand":
        meta.node.prefix = () =>
          h("div", { class: "i-mdi-folder-open" })
        break
      case "collapse":
        meta.node.prefix = () =>
          h("div", { class: "i-mdi-folder" })
        break
    }
  }

  // #region Drag ========================================
  function findSiblingsAndIndex(
    node: TreeOption,
    nodes?: TreeOption[],
  ): [TreeOption[], number] | [undefined, undefined] {
    if (!nodes) return [undefined, undefined]
    for (let i = 0; i < nodes.length; ++i) {
      const siblingNode = nodes[i]
      if (siblingNode.key === node.key) return [nodes, i]
      const [siblings, index] = findSiblingsAndIndex(node, siblingNode.children)
      if (siblings && index !== null) return [siblings, index]
    }
    return [undefined, undefined]
  }

  const handleDrop = ({ node, dragNode, dropPosition }: TreeDropInfo) => {
    const [dragNodeSiblings, dragNodeIndex] = findSiblingsAndIndex(
      dragNode,
      data.value,
    )

    if (dragNodeSiblings === undefined) return
    dragNodeSiblings.splice(dragNodeIndex, 1)
    if (dropPosition === "inside") {
      if (node.children) {
        node.children.unshift(dragNode)
      } else {
        node.children = [dragNode]
      }
    } else if (dropPosition === "before") {
      const [nodeSiblings, nodeIndex] = findSiblingsAndIndex(
        node,
        data.value,
      )
      if (nodeSiblings === undefined || nodeIndex === undefined) return
      nodeSiblings.splice(nodeIndex, 0, dragNode)
    } else if (dropPosition === "after") {
      const [nodeSiblings, nodeIndex] = findSiblingsAndIndex(
        node,
        data.value,
      )
      if (nodeSiblings === undefined || nodeIndex === undefined) return
      nodeSiblings.splice(nodeIndex + 1, 0, dragNode)
    }
    data.value = Array.from(data.value)
  }
  // #endregion

  return {
    data,
    searchKey,
    selectedKey,
    refresh,
    handleDrop,
    newFile,
    list,
    trash: trashFile,
    animationOnExpand,
    get: getTemplate,
    save,
    onSelect,
  }
}