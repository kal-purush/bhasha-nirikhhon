"use client"

import { Row, Section, Table } from "./element"
import { drawCircle, drawPolygon, drawText } from "./draw/drawCanvas2D"
import { drawCircleListByWebGL, drawPolygonListByWebGL } from "./draw/drawWebGL"
import { Circle, Polygon, Text } from "./shapeType"

export type CanvasType = "2d" | "webgl"

export class SeatsCanvas {
    scale = 1
    offsetX = 0
    offsetY = 0
    isDragging = false
    startX = 0
    startY = 0
    canvas!: HTMLCanvasElement
    ctx!: CanvasRenderingContext2D | WebGL2RenderingContext
    elements: any[] = []
    circles: Circle[] = []
    texts: Text[] = []
    polygons: Polygon[] = []
    colorMap: Record<string, any> = {}
    private animationFrameId: number | null = null
    elementsTotalWidth = 0
    elementsTotalHeight = 0
    elementsCenterX = 0
    elementsCenterY = 0

    constructor({
        canvasBox,
        type = "2d",
    }: {
        width?: number
        height?: number
        canvasBox?: HTMLElement | undefined
        type?: CanvasType
    }) {
        if (type === "2d") {
            this.initCanvas({ canvasBox })
        } else if (type === "webgl") {
            this.initCanvasWebGL({ canvasBox: canvasBox as HTMLElement })
        }
    }

    initCanvas({
        canvasBox = undefined,
    }: {
        width?: number
        height?: number
        canvasBox?: HTMLElement | undefined
    }) {
        const canvas = document.createElement("canvas")
        let width = 500
        let height = 300
        if (canvasBox) {
            width = canvasBox.clientWidth
            height = canvasBox.clientHeight
            canvasBox.innerHTML = ""
            canvasBox.appendChild(canvas)
        } else {
            // throw new Error("canvasBox is required")
            console.log("canvasBox is required")
        }
        // Canvas settings
        const ratio = window.devicePixelRatio * 1.5 || 1
        // Actual rendering pixels
        canvas.width = width * ratio
        canvas.height = height * ratio
        // Control display size
        canvas.style.width = `${width}px`
        canvas.style.height = `${height}px`

        const ctx = canvas.getContext("2d") as CanvasRenderingContext2D
        ctx.scale(ratio, ratio)
        this.canvas = canvas
        this.ctx = ctx

        // Add resize handler
        // window.addEventListener("resize", this.handleResize.bind(this))
        this.bindCanvasControl()
    }

    initCanvasWebGL({ canvasBox }: { canvasBox: HTMLElement }) {
        const canvas = document.createElement("canvas")
        let width = 500
        let height = 300
        if (canvasBox) {
            width = canvasBox.clientWidth
            height = canvasBox.clientHeight
            canvasBox.innerHTML = ""
            canvasBox.appendChild(canvas)
        } else {
            // throw new Error("canvasBox is required")
            console.log("canvasBox is required")
        }

        // Canvas settings
        const ratio = window.devicePixelRatio * 1.5 || 1
        // Actual rendering pixels
        canvas.width = width * ratio
        canvas.height = height * ratio
        // Control display size
        canvas.style.width = `${width}px`
        canvas.style.height = `${height}px`

        const ctx = canvas.getContext("webgl2") as WebGL2RenderingContext
        // ctx.scale(ratio, ratio)
        this.canvas = canvas
        this.ctx = ctx

        // Add resize handler
        window.addEventListener("resize", this.handleResize.bind(this))
        this.bindCanvasControl()

        this.draw()
    }

    private handleResize() {
        if (!this.canvas) return

        // const canvasBox = this.canvas.parentElement
        // if (!canvasBox) return

        // const width = canvasBox.clientWidth
        // const height = canvasBox.clientHeight

        // // Update canvas size
        // const ratio = window.devicePixelRatio * 1.5 || 1
        // this.canvas.width = width * ratio
        // this.canvas.height = height * ratio
        // this.canvas.style.width = `${width}px`
        // this.canvas.style.height = `${height}px`

        // // Reset high DPI scaling
        // const ctx = this.canvas.getContext("2d") as CanvasRenderingContext2D
        // ctx.scale(ratio, ratio)

        // // Redraw using new size
        // this.autoFit()
    }

    bindCanvasControl() {
        this.canvas.addEventListener("contextmenu", function (e) {
            e.preventDefault()
            return false
        })
        // Handle mouse down event to start dragging
        this.canvas.addEventListener("mousedown", (event) => {
            event.preventDefault()
            if (event.button === 0 || event.button === 2) {
                // Left button
                this.isDragging = true
                this.startX = event.clientX - this.offsetX
                this.startY = event.clientY - this.offsetY
            }
        })

        // Handle mouse move event to drag
        this.canvas.addEventListener("mousemove", (event) => {
            if (this.isDragging) {
                this.offsetX = event.clientX - this.startX
                this.offsetY = event.clientY - this.startY
                this.draw()
            }
        })

        this.canvas.addEventListener("mouseup", () => {
            this.isDragging = false
        })

        this.canvas.addEventListener("mouseleave", () => {
            this.isDragging = false
        })

        // Handle mouse wheel event to zoom
        this.canvas.addEventListener("wheel", (event) => {
            event.preventDefault()
            const mouseX =
                event.clientX - this.canvas.getBoundingClientRect().left
            const mouseY =
                event.clientY - this.canvas.getBoundingClientRect().top

            const zoom = event.deltaY > 0 ? 0.9 : 1.1 // Determine zoom factor
            const newScale = this.scale * zoom // Calculate new scale
            this.scale = newScale

            // Calculate offset difference before and after zoom
            const dx = (mouseX - this.offsetX) * (zoom - 1)
            const dy = (mouseY - this.offsetY) * (zoom - 1)

            // Update offset and scale
            this.offsetX -= dx
            this.offsetY -= dy
            // Update scale
            this.draw()
        })
    }

    loadDataRoot(data: any) {
        const { subChart, categories } = data

        this.colorMap = Object.fromEntries(
            categories.list.map((v: any) => [
                v.key,
                { color: v.color, label: v.label },
            ])
        )

        this.elementsTotalWidth = subChart.width
        this.elementsTotalHeight = subChart.height
        this.elementsCenterX = subChart?.focalPoint?.x || 0
        this.elementsCenterY = subChart?.focalPoint?.y || 0
        this.autoFit()
        this.loadData(subChart)
    }

    autoFit() {
        if (!this.canvas) return

        // Get canvas dimensions
        const canvasWidth = this.canvas.clientWidth
        const canvasHeight = this.canvas.clientHeight
        const padding = 20
        const scaleX = (canvasWidth - padding * 2) / this.elementsTotalWidth
        const scaleY = (canvasHeight - padding * 2) / this.elementsTotalHeight

        this.scale = Math.min(scaleX, scaleY, 1)
        this.offsetX = (canvasWidth - this.elementsTotalWidth * this.scale) / 2
        this.offsetY =
            (canvasHeight - this.elementsTotalHeight * this.scale) / 2

        this.draw()
    }

    loadData(chartdata: any) {
        const { sections, rows, tables } = chartdata

        this.addElements(sections.map((v) => new Section(v, this)))
        this.addElements(rows.map((v) => new Row(v, this)))
        this.addElements(tables.map((v) => new Table(v, this)))
    }

    addElements(elements: any[]) {
        for (const e of elements) {
            this.elements.push(e)
        }
    }

    drawCanvas2D(ctx: CanvasRenderingContext2D) {
        // clear canvas
        ctx.clearRect(0, 0, this.canvas.width, this.canvas.height)
        ctx.save()
        // translate and scale canvas
        ctx.translate(this.offsetX, this.offsetY)
        ctx.scale(this.scale, this.scale)

        for (const polygon of this.polygons) {
            drawPolygon(ctx, polygon)
        }
        for (const circle of this.circles) {
            drawCircle(ctx, circle)
        }
        for (const text of this.texts) {
            drawText(ctx, text)
        }

        ctx.restore()
    }

    drawWebGL(ctx: WebGL2RenderingContext) {
        // clear canvas
        ctx.clearColor(0, 0, 0, 0)
        ctx.clear(ctx.COLOR_BUFFER_BIT)

        drawPolygonListByWebGL(
            ctx,
            this.polygons,
            this.offsetX,
            this.offsetY,
            this.scale
        )

        drawCircleListByWebGL(
            ctx,
            this.circles,
            this.offsetX,
            this.offsetY,
            this.scale
        )
    }

    draw() {
        if (this.animationFrameId) return

        this.animationFrameId = requestAnimationFrame(() => {
            if (this.ctx instanceof CanvasRenderingContext2D) {
                this.drawCanvas2D(this.ctx)
            } else if (this.ctx instanceof WebGL2RenderingContext) {
                this.drawWebGL(this.ctx)
            }

            this.animationFrameId = null
        })
    }
}