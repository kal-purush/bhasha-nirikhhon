import { Polyline } from '@antv/g';
import { PivotSheet, S2Options, SpreadSheet, Node, S2Event  } from '@antv/s2';
import { get } from 'lodash';

// 存储已绘制的连接线，用于销毁
const dottedTreeLines: Polyline[] = [];
/**
 * 销毁已存在的虚线
 */
function destroyDottedLines(lines: Polyline[] = []) {
	while (lines.length) {
		lines?.pop()?.destroy();
	}
}

/**
 * 获取列头的高度
 */
function getColHeaderHeight(s2: SpreadSheet) {
	return s2.facet.getColNodes(0)?.[0]?.hierarchy?.height;
}

/**
 * 获取滚动高度
 */
function getOffsetHeight(s2: SpreadSheet) {
	return s2.facet.getScrollOffset().scrollY;
}
/**
 * 获取左侧X轴滚动距离
 */
function getHeaderOffsetWidth(s2: SpreadSheet) {
	return s2.facet.getScrollOffset().rowHeaderScrollX;
}

/**
 * 获取树级行头嵌套深度
 */
function getRowHeaderTreeDepth(s2: SpreadSheet) {
	return s2.facet.getRowNodes(0)?.[0]?.hierarchy?.maxLevel;
}

/**
 * 获取视口高度（列头 + 数值区域）
 */
function getViewportHeight(s2: SpreadSheet) {
	return s2.facet.panelBBox.viewportHeight + getColHeaderHeight(s2);
}

/**
 * 获取 tree icon 的信息
 */
function getTreeIconCfg(node: Node) {
	if (
		node &&
		get(node, 'belongsCell.treeIcon.cfg') &&
    !get(node, 'belongsCell.treeIcon.cfg.destroyed')
	) {
		return get(node, 'belongsCell.treeIcon.cfg');
	}

	return {
		'x': 8 + node.level * 14,
		'y': node.y + node.height / 2 - 10 / 2,
		'width': 10,
		'height': 10
	};
}

/**
 * 画线
 */
export function drawDottedLines(s2: SpreadSheet) {

	destroyDottedLines(dottedTreeLines);
	const canvas = s2.container;
	const colHeaderHeight = getColHeaderHeight(s2);
	const viewportHeight = getViewportHeight(s2);
	const offsetHeight = getOffsetHeight(s2);
	const headerOffsetWidth = getHeaderOffsetWidth(s2);
	const rowHeaderTreeDepth = getRowHeaderTreeDepth(s2);

	for (let i = 0; i <= rowHeaderTreeDepth; i++) {
		// 获取当前层级的节点
		const rowNodes = s2.facet.getRowNodes(i);

		rowNodes.forEach((rowNode) => {
			if (rowNode.children.length) {
				// 避免重复画线导致的颜色过深
				const childs: Node[] = [];

				rowNode.children.forEach((child) => {
					const rowNodeTreeIconCfg = getTreeIconCfg(rowNode);
					const childTreeIconCfg = getTreeIconCfg(child);

					if (rowNodeTreeIconCfg && childTreeIconCfg) {
						const x1 =
							rowNode.x + rowNodeTreeIconCfg.x + rowNodeTreeIconCfg.width / 2 - headerOffsetWidth;
						let y1 =
                colHeaderHeight +
                rowNodeTreeIconCfg.y +
                rowNodeTreeIconCfg.height;

						if (childs?.length > 0) {
							const preChild = childs?.pop();
							if (preChild){
								const preChildTreeIconCfg = getTreeIconCfg(preChild);
								y1 =
                    colHeaderHeight +
                    preChildTreeIconCfg.y +
                    preChildTreeIconCfg.height / 2;
							}
						}

						childs.push(child);
						const x2 = child.x + childTreeIconCfg.x - headerOffsetWidth;
						const y2 =
                colHeaderHeight +
                childTreeIconCfg.y +
                childTreeIconCfg.height / 2;
						const points = [
							[
								x1,
								Math.min(
									Math.max(y1 - offsetHeight, colHeaderHeight),
									viewportHeight
								)
							],
							[
								x1,
								Math.min(
									Math.max(y2 - offsetHeight, colHeaderHeight),
									viewportHeight
								)
							]
						];

						if (
							y2 - offsetHeight >= colHeaderHeight &&
                y2 - offsetHeight <= viewportHeight
						) {
							points.push([x2, y2 - offsetHeight]);
						}

						const dottedLine = canvas.appendChild(
							new Polyline({
								'style': {
									'points': points as any,
									'stroke': '#000',
									'lineDash': [2, 3],
									'lineWidth': 1,
									'lineJoin': 'round',
									'zIndex': 9999
								}
							})
						);

						dottedTreeLines.push(dottedLine);
					}
				});
			}
		});
	}
}



fetch(
  'https://gw.alipayobjects.com/os/bmw-prod/2a5dbbc8-d0a7-4d02-b7c9-34f6ca63cff6.json',
)
  .then((res) => res.json())
  .then(async (dataCfg) => {
    const container = document.getElementById('container');

    const s2Options: S2Options = {
      width: 600,
      height: 480,
      hierarchyType: 'tree',
    };

    const s2 = new PivotSheet(container, dataCfg, s2Options);

    s2.on(S2Event.LAYOUT_AFTER_RENDER, () => {
      drawDottedLines(s2);
    });
    s2.on(S2Event.GLOBAL_SCROLL, () => {
      drawDottedLines(s2);
    });
    s2.on(S2Event.LAYOUT_DESTROY, () => {
      destroyDottedLines(dottedTreeLines);
    });

    await s2.render();
  });