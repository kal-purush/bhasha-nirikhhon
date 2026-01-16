import "./styles.css";
import $ from "jquery";
// Bootstrap スタイルシート
import "bootstrap/dist/css/bootstrap.min.css";
// Bootstrap の JavaScript の機能
import "bootstrap";
// SQL整形
import sqlFormatter from "sql-formatter";

$(() => {
  init.bind(this);
  findMax.bind(this);

  $('[data-toggle="tooltip"]').tooltip();

  // 初期表示
  init();
});

window.run = () => {
  let src = $("#src").val();
  const max = findMax(src);

  if (max === 0) {
    $("#output").val("not found $");
    return;
  }

  // $n部分を値に置換
  for (let num = 1; num <= max; num++) {
    try {
      // 置換後の文字列を抽出
      const dest = new RegExp("\\$" + num + "\\s=\\s'([^']*)", "g").exec(
        src
      )[1];
      // 置換処理
      src = src.replace(new RegExp("\\$" + num, "g"), "'" + dest + "'");
    } catch (e) {
      $("#output").val("error: " + e);
    }
  }

  try {
    // クエリ以外除去 2020...部分
    const remove1 = new RegExp("202(.*)pdo_stmt_\\w+:\\s", "g").exec(src);
    src = src.replace(remove1[0], "");
    const remove2 = new RegExp("202(.*)parameters(.*)", "g").exec(src);
    src = src.replace(remove2[0], "");
  } catch (e) {
    $("#output").val("error: " + e);
  }

  $("#output").val(sqlFormatter.format(src));
};

window.copy = () => {
  var copyTarget = document.getElementById("output");
  copyTarget.select();
  document.execCommand("Copy");

  // selection clear
  var copyTarget1 = document.getElementById("empty");
  copyTarget1.select();

  setTimeout(() => {
    $('[data-toggle="tooltip"]').tooltip("hide");
    // window.getSelection().removeAllRanges();
  }, 1000);
};

function init() {
  $("#run").prop("disabled", true);

  $("#src").attr(
    "placeholder",
    `2020-05-26 12:47:32.852 JST [1404] LOG:  execute pdo_stmt_0000009c: SELECT * FROM xxx WHERE name=$1 AND address=$2
2020-05-26 12:47:32.852 JST [1404] DETAIL:  parameters: $1 = 'hykisk', $2 = 'Osaka'`
  );

  $("#output").val(
    `SELECT
  *
FROM
  xxx
WHERE
  name = 'hykisk'
  AND address = 'Osaka'
  `
  );

  //フォーカス時にplaceholderを消す
  $("#src").focus(function() {
    $("#src").attr("placeholder", "");
    $("#run").prop("disabled", false);
  });
}

function findMax(src) {
  let result = 0;

  if (src === "") {
    return result;
  }
  var regex = new RegExp("\\$(\\d*)", "g");
  let matches = [];
  // eslint-disable-next-line no-cond-assign
  while ((matches = regex.exec(src)) !== null) {
    result = result < matches[1] ? matches[1] : result;
  }
  return result;
}