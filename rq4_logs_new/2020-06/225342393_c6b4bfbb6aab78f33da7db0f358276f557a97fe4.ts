import * as idAndToken from "./idAndToken";
import * as name from "./name";
import * as customType from "./customType";
import {
  Type,
  Maybe,
  CustomTypeDefinition,
  CustomTypeDefinitionBody,
} from "@narumincho/type/distribution/data";

export const customTypeList: ReadonlyArray<CustomTypeDefinition> = [
  {
    name: name.time,
    description:
      "日時. 0001-01-01T00:00:00.000Z to 9999-12-31T23:59:59.999Z 最小単位はミリ秒. ミリ秒の求め方は day*1000*60*60*24 + millisecond",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "day",
        description: "1970-01-01からの経過日数. マイナスになることもある",
        type: Type.Int32,
      },
      {
        name: "millisecond",
        description: "日にちの中のミリ秒. 0 to 86399999 (=1000*60*60*24-1)",
        type: Type.Int32,
      },
    ]),
  },
  {
    name: name.requestLogInUrlRequestData,
    description: "ログインのURLを発行するために必要なデータ",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "openIdConnectProvider",
        description: "ログインに使用するプロバイダー",
        type: customType.openIdConnectProvider,
      },
      {
        name: "urlData",
        description: "ログインした後に返ってくるURLに必要なデータ",
        type: customType.urlData,
      },
    ]),
  },
  {
    name: name.openIdConnectProvider,
    description:
      "ソーシャルログインを提供するプロバイダー (例: Google, GitHub)",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "Google",
        description:
          "Google ( https://developers.google.com/identity/sign-in/web/ )",
        parameter: Maybe.Nothing(),
      },
      {
        name: "GitHub",
        description:
          "GitHub ( https://developer.github.com/v3/guides/basics-of-authentication/ )",
        parameter: Maybe.Nothing(),
      },
    ]),
  },
  {
    name: name.urlData,
    description:
      "デバッグモードかどうか,言語とページの場所. URLとして表現されるデータ. Googleなどの検索エンジンの都合( https://support.google.com/webmasters/answer/182192?hl=ja )で,URLにページの言語を入れて,言語ごとに別のURLである必要がある. デバッグ時のホスト名は http://localhost になる",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "clientMode",
        description: "クライアントモード",
        type: customType.clientMode,
      },
      {
        name: "location",
        description: "場所",
        type: customType.location,
      },
      {
        name: "language",
        description: "言語",
        type: customType.language,
      },
    ]),
  },
  {
    name: name.clientMode,
    description: "デバッグモードか, リリースモード",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "DebugMode",
        description: "デバッグモード. オリジンは http://localshot:2520",
        parameter: Maybe.Nothing(),
      },
      {
        name: "Release",
        description: "リリースモード. オリジンは https://definy.app ",
        parameter: Maybe.Nothing(),
      },
    ]),
  },
  {
    name: name.location,
    description:
      "DefinyWebアプリ内での場所を示すもの. URLから求められる. URLに変換できる",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "Home",
        description: "最初のページ",
        parameter: Maybe.Nothing(),
      },
      {
        name: "CreateProject",
        description: "プロジェクト作成画面",
        parameter: Maybe.Nothing(),
      },
      {
        name: "Project",
        description: "プロジェクトの詳細ページ",
        parameter: Maybe.Just(idAndToken.projectId),
      },
      {
        name: "User",
        description: "ユーザーの詳細ページ",
        parameter: Maybe.Just(idAndToken.userId),
      },
      {
        name: "Idea",
        description: "アイデア詳細ページ",
        parameter: Maybe.Just(idAndToken.ideaId),
      },
      {
        name: "Suggestion",
        description: "提案のページ",
        parameter: Maybe.Just(idAndToken.suggestionId),
      },
      {
        name: "About",
        description: "Definyについて説明したページ",
        parameter: Maybe.Nothing(),
      },
      {
        name: "Debug",
        description: "デバッグページ",
        parameter: Maybe.Nothing(),
      },
    ]),
  },
  {
    name: name.language,
    description: "英語,日本語,エスペラント語などの言語",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "Japanese",
        description: "日本語",
        parameter: Maybe.Nothing(),
      },
      {
        name: "English",
        description: "英語",
        parameter: Maybe.Nothing(),
      },
      {
        name: "Esperanto",
        description: "エスペラント語",
        parameter: Maybe.Nothing(),
      },
    ]),
  },
  {
    name: name.user,
    description: "ユーザーのデータのスナップショット",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "name",
        description:
          "ユーザー名. 表示される名前. 他のユーザーとかぶっても良い. 絵文字も使える. 全角英数は半角英数,半角カタカナは全角カタカナ, (株)の合字を分解するなどのNFKCの正規化がされる. U+0000-U+0019 と U+007F-U+00A0 の範囲の文字は入らない. 前後に空白を含められない. 間の空白は2文字以上連続しない. 文字数のカウント方法は正規化されたあとのCodePoint単位. Twitterと同じ, 1文字以上50文字以下",
        type: Type.String,
      },
      {
        name: "imageHash",
        description: "プロフィール画像",
        type: idAndToken.imageToken,
      },
      {
        name: "introduction",
        description:
          "自己紹介文. 改行文字を含めることができる. Twitterと同じ 0～160文字",
        type: Type.String,
      },
      {
        name: "createTime",
        description: "Definyでユーザーが作成された日時",
        type: customType.time,
      },
      {
        name: "likeProjectIdList",
        description: "プロジェクトに対する いいね",
        type: Type.List(idAndToken.projectId),
      },
      {
        name: "developProjectIdList",
        description: "開発に参加した (書いたコードが使われた) プロジェクト",
        type: Type.List(idAndToken.projectId),
      },
      {
        name: "commentIdeaIdList",
        description: "コメントをしたアイデア",
        type: Type.List(idAndToken.ideaId),
      },
      {
        name: "getTime",
        description: "取得日時",
        type: customType.time,
      },
    ]),
  },
  {
    name: name.idAndData,
    description: "データを識別するIdとデータ",
    typeParameterList: ["id", "data"],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "id",
        description: "ID",
        type: Type.Parameter("id"),
      },
      {
        name: "data",
        description: "データ",
        type: Type.Parameter("data"),
      },
    ]),
  },
  {
    name: name.project,
    description: "プロジェクト",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "name",
        description: "プロジェクト名",
        type: Type.String,
      },
      {
        name: "iconHash",
        description: "プロジェクトのアイコン画像",
        type: idAndToken.imageToken,
      },
      {
        name: "imageHash",
        description: "プロジェクトのカバー画像",
        type: idAndToken.imageToken,
      },
      {
        name: "createTime",
        description: "作成日時",
        type: customType.time,
      },
      {
        name: "createUserId",
        description: "作成アカウント",
        type: idAndToken.userId,
      },
      {
        name: "updateTime",
        description: "更新日時",
        type: customType.time,
      },
      {
        name: "getTime",
        description: "取得日時",
        type: customType.time,
      },
      {
        name: "partIdList",
        description: "所属しているのパーツのIDのリスト",
        type: Type.List(idAndToken.partId),
      },
      {
        name: "typePartIdList",
        description: "所属している型パーツのIDのリスト",
        type: Type.List(idAndToken.typePartId),
      },
    ]),
  },
  {
    name: name.idea,
    description: "アイデア",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "name",
        description: "アイデア名",
        type: Type.String,
      },
      {
        name: "createUserId",
        description: "言い出しっぺ",
        type: idAndToken.userId,
      },
      {
        name: "createTime",
        description: "作成日時",
        type: customType.time,
      },
      {
        name: "projectId",
        description: "対象のプロジェクト",
        type: idAndToken.projectId,
      },
      {
        name: "itemList",
        description: "アイデアの要素",
        type: Type.List(customType.ideaItem),
      },
      {
        name: "updateTime",
        description: "更新日時",
        type: customType.time,
      },
      {
        name: "getTime",
        description: "取得日時",
        type: customType.time,
      },
    ]),
  },
  {
    name: name.ideaItem,
    description: "アイデアのコメント",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "createUserId",
        description: "作成者",
        type: idAndToken.userId,
      },
      {
        name: "createTime",
        description: "作成日時",
        type: customType.time,
      },
      {
        name: "body",
        description: "本文",
        type: customType.itemBody,
      },
    ]),
  },
  {
    name: name.itemBody,
    description: "アイデアのアイテム",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "Comment",
        description: "文章でのコメントをした",
        parameter: Maybe.Just(Type.String),
      },
      {
        name: "SuggestionCreate",
        description: "提案を作成した",
        parameter: Maybe.Just(idAndToken.suggestionId),
      },
      {
        name: "SuggestionToApprovalPending",
        description: "提案を承認待ちにした",
        parameter: Maybe.Just(idAndToken.suggestionId),
      },
      {
        name: "SuggestionCancelToApprovalPending",
        description: "承認待ちをキャンセルした",
        parameter: Maybe.Just(idAndToken.suggestionId),
      },
      {
        name: "SuggestionApprove",
        description: "提案を承認した",
        parameter: Maybe.Just(idAndToken.suggestionId),
      },
      {
        name: "SuggestionReject",
        description: "提案を拒否した",
        parameter: Maybe.Just(idAndToken.suggestionId),
      },
      {
        name: "SuggestionCancelRejection",
        description: "提案の拒否をキャンセルした",
        parameter: Maybe.Just(idAndToken.suggestionId),
      },
    ]),
  },
  {
    name: name.suggestion,
    description: "提案",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "name",
        description: "変更概要",
        type: Type.String,
      },
      {
        name: "createUserId",
        description: "作成者",
        type: idAndToken.userId,
      },
      {
        name: "reason",
        description: "変更理由",
        type: Type.String,
      },
      {
        name: "state",
        description: "承認状態",
        type: customType.suggestionState,
      },
      {
        name: "changeList",
        description: "変更",
        type: Type.List(customType.change),
      },
      {
        name: "projectId",
        description: "変更をするプロジェクト",
        type: idAndToken.projectId,
      },
      {
        name: "ideaId",
        description: "投稿したアイデアID",
        type: idAndToken.ideaId,
      },
      {
        name: "updateTime",
        description: "更新日時",
        type: customType.time,
      },
      {
        name: "getTime",
        description: "取得日時",
        type: customType.time,
      },
    ]),
  },
  {
    name: name.suggestionState,
    description: "提案の状況",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "Creating",
        description: "作成中",
        parameter: Maybe.Nothing(),
      },
      {
        name: "ApprovalPending",
        description: "承認待ち",
        parameter: Maybe.Nothing(),
      },
      {
        name: "Approved",
        description: "承認済み",
        parameter: Maybe.Nothing(),
      },
      {
        name: "Rejected",
        description: "拒否された",
        parameter: Maybe.Nothing(),
      },
    ]),
  },
  {
    name: name.change,
    description: "変更点",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "ProjectName",
        description: "プロジェクト名の変更",
        parameter: Maybe.Just(Type.String),
      },
      {
        name: "AddPart",
        description: "パーツの追加",
        parameter: Maybe.Just(customType.addPart),
      },
    ]),
  },
  {
    name: name.addPart,
    description: "パーツを追加するのに必要なもの",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "id",
        description: "ブラウザで生成した今回作成した提案内で参照するためのID",
        type: Type.Int32,
      },
      {
        name: "name",
        description: "新しいパーツの名前",
        type: Type.String,
      },
      {
        name: "description",
        description: "新しいパーツの説明",
        type: Type.String,
      },
      {
        name: "type",
        description: "新しいパーツの型",
        type: customType.suggestionType,
      },
      {
        name: "expr",
        description: "新しいパーツの式",
        type: customType.suggestionExpr,
      },
    ]),
  },
  {
    name: name.suggestionType,
    description: "ChangeのAddPartなどで使われる提案で作成した型を使えるType",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "Function",
        description: "関数",
        parameter: Maybe.Just(customType.suggestionTypeInputAndOutput),
      },
      {
        name: "TypePartWithParameter",
        description: "提案前に作られた型パーツとパラメーター",
        parameter: Maybe.Just(customType.typePartWithSuggestionTypeParameter),
      },
      {
        name: "SuggestionTypePartWithParameter",
        description: "提案時に作られた型パーツとパラメーター",
        parameter: Maybe.Just(
          customType.suggestionTypePartWithSuggestionTypeParameter
        ),
      },
    ]),
  },
  {
    name: name.suggestionTypeInputAndOutput,
    description: "",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "inputType",
        description: "入力の型",
        type: customType.suggestionType,
      },
      {
        name: "outputType",
        description: "出力の型",
        type: customType.suggestionType,
      },
    ]),
  },
  {
    name: name.typePartWithSuggestionTypeParameter,
    description: "",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "typePartId",
        description: "型の参照",
        type: idAndToken.typePartId,
      },
      {
        name: "parameter",
        description: "型のパラメーター",
        type: Type.List(customType.suggestionType),
      },
    ]),
  },
  {
    name: name.suggestionTypePartWithSuggestionTypeParameter,
    description: "",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "suggestionTypePartIndex",
        description: "提案内での定義した型パーツのID",
        type: Type.Int32,
      },
      {
        name: "parameter",
        description: "型のパラメーター",
        type: Type.List(customType.suggestionType),
      },
    ]),
  },
  {
    name: name.suggestionExpr,
    description: "提案時に含まれるパーツを参照できる式",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "Kernel",
        description: "Definyだけでは表現できない式",
        parameter: Maybe.Just(customType.kernelExpr),
      },
      {
        name: "Int32Literal",
        description: "32bit整数",
        parameter: Maybe.Just(Type.Int32),
      },
      {
        name: "PartReference",
        description: "パーツの値を参照",
        parameter: Maybe.Just(idAndToken.partId),
      },
      {
        name: "SuggestionPartReference",
        description: "提案内で定義されたパーツのID",
        parameter: Maybe.Just(Type.Int32),
      },
      {
        name: "LocalPartReference",
        description: "ローカルパーツの参照",
        parameter: Maybe.Just(customType.localPartReference),
      },
      {
        name: "TagReference",
        description: "タグを参照",
        parameter: Maybe.Just(customType.TagReference),
      },
      {
        name: "SuggestionTagReference",
        description: "提案内で定義された型のタグ",
        parameter: Maybe.Just(customType.suggestionTagReference),
      },
      {
        name: "FunctionCall",
        description: "関数呼び出し (中に含まれる型はSuggestionExpr)",
        parameter: Maybe.Just(customType.suggestionFunctionCall),
      },
      {
        name: "Lambda",
        description: "ラムダ",
        parameter: Maybe.Just(Type.List(customType.suggestionLambdaBranch)),
      },
      {
        name: "Blank",
        description: "空白",
        parameter: Maybe.Nothing(),
      },
    ]),
  },
  {
    name: name.suggestionTagReference,
    description: "提案内で定義された型のタグ",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "suggestionTypePartIndex",
        description: "提案内での定義した型パーツの番号",
        type: Type.Int32,
      },
      {
        name: "tagIndex",
        description: "タグIndex",
        type: Type.Int32,
      },
    ]),
  },
  {
    name: name.suggestionFunctionCall,
    description: "関数呼び出し (中に含まれる型はSuggestionExpr)",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "function",
        description: "関数",
        type: customType.suggestionExpr,
      },
      {
        name: "parameter",
        description: "パラメーター",
        type: customType.suggestionExpr,
      },
    ]),
  },
  {
    name: name.suggestionLambdaBranch,
    description: "suggestionExprの入ったLambdaBranch",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "condition",
        description: "入力値の条件を書くところ",
        type: customType.condition,
      },
      {
        name: "description",
        description: "ブランチの説明",
        type: Type.String,
      },
      {
        name: "localPartList",
        description: "",
        type: Type.List(customType.suggestionBranchPartDefinition),
      },
      {
        name: "expr",
        description: "式",
        type: customType.suggestionExpr,
      },
    ]),
  },
  {
    name: name.suggestionBranchPartDefinition,
    description:
      "ラムダのブランチで使えるパーツを定義する部分 (SuggestionExpr バージョン)",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "localPartId",
        description: "ローカルパーツID",
        type: idAndToken.localPartId,
      },
      {
        name: "name",
        description: "ブランチパーツの名前",
        type: Type.String,
      },
      {
        name: "description",
        description: "ブランチパーツの説明",
        type: Type.String,
      },
      {
        name: "type",
        description: "ローカルパーツの型",
        type: customType.suggestionType,
      },
      {
        name: "expr",
        description: "ローカルパーツの式",
        type: customType.suggestionExpr,
      },
    ]),
  },
  {
    name: name.typePart,
    description: "型パーツ",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "name",
        description: "型パーツの名前",
        type: Type.String,
      },
      {
        name: "parentList",
        description: "この型パーツの元",
        type: Type.List(idAndToken.partId),
      },
      {
        name: "description",
        description: "型パーツの説明",
        type: Type.String,
      },
      {
        name: "projectId",
        description: "所属しているプロジェクトのID",
        type: idAndToken.projectId,
      },
      {
        name: "createSuggestionId",
        description: "この型パーツが作成された提案",
        type: idAndToken.suggestionId,
      },
      {
        name: "getTime",
        description: "取得日時",
        type: customType.time,
      },
      {
        name: "body",
        description: "定義本体",
        type: customType.typePartBody,
      },
    ]),
  },
  {
    name: name.part,
    description: "パーツの定義",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "name",
        description: "パーツの名前",
        type: Type.String,
      },
      {
        name: "parentList",
        description: "このパーツの元",
        type: Type.List(idAndToken.partId),
      },
      {
        name: "description",
        description: "パーツの説明",
        type: Type.String,
      },
      {
        name: "type",
        description: "パーツの型",
        type: customType.type,
      },
      {
        name: "expr",
        description: "パーツの式",
        type: customType.expr,
      },
      {
        name: "projectId",
        description: "所属しているプロジェクトのID",
        type: idAndToken.projectId,
      },
      {
        name: "createSuggestionId",
        description: "このパーツが作成された提案",
        type: idAndToken.suggestionId,
      },
      {
        name: "getTime",
        description: "取得日時",
        type: customType.time,
      },
    ]),
  },
  {
    name: name.typePartBody,
    description: "型の定義本体",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "Product",
        description: "直積型",
        parameter: Maybe.Just(Type.List(customType.member)),
      },
      {
        name: "Sum",
        description: "直和型",
        parameter: Maybe.Just(Type.List(customType.pattern)),
      },
      {
        name: "Kernel",
        description: "Definyだけでは表現できないデータ型",
        parameter: Maybe.Just(customType.typePartBodyKernel),
      },
    ]),
  },
  {
    name: name.member,
    description: "直積型のメンバー",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "name",
        description: "メンバー名",
        type: Type.String,
      },
      {
        name: "description",
        description: "説明文",
        type: Type.String,
      },
      {
        name: "type",
        description: "メンバー値の型",
        type: customType.type,
      },
    ]),
  },
  {
    name: name.pattern,
    description: "直積型のパターン",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "name",
        description: "タグ名",
        type: Type.String,
      },
      {
        name: "description",
        description: "説明文",
        type: Type.String,
      },
      {
        name: "parameter",
        description: "パラメーター",
        type: Type.Maybe(customType.type),
      },
    ]),
  },
  {
    name: name.typePartBodyKernel,
    description: "Definyだけでは表現できないデータ型",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "Int32",
        description: "32bit整数",
        parameter: Maybe.Nothing(),
      },
      {
        name: "List",
        description: "リスト",
        parameter: Maybe.Nothing(),
      },
    ]),
  },
  {
    name: name.type,
    description: "型",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "Function",
        description: "関数",
        parameter: Maybe.Just(customType.typeInputAndOutput),
      },
      {
        name: "TypePartWithParameter",
        description: "型パーツと, パラメーターのリスト",
        parameter: Maybe.Just(customType.typePartIdWithParameter),
      },
    ]),
  },
  {
    name: name.typeInputAndOutput,
    description: "",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "inputType",
        description: "入力の型",
        type: customType.type,
      },
      {
        name: "outputType",
        description: "出力の型",
        type: customType.type,
      },
    ]),
  },
  {
    name: name.typePartIdWithParameter,
    description: "",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "typePartId",
        description: "型の参照",
        type: idAndToken.typePartId,
      },
      {
        name: "parameter",
        description: "型のパラメーター",
        type: Type.List(customType.type),
      },
    ]),
  },
  {
    name: name.expr,
    description: "式",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "Kernel",
        description: "Definyだけでは表現できない式",
        parameter: Maybe.Just(customType.kernelExpr),
      },
      {
        name: "Int32Literal",
        description: "32bit整数",
        parameter: Maybe.Just(Type.Int32),
      },
      {
        name: "PartReference",
        description: "パーツの値を参照",
        parameter: Maybe.Just(idAndToken.partId),
      },
      {
        name: "LocalPartReference",
        description: "ローカルパーツの参照",
        parameter: Maybe.Just(customType.localPartReference),
      },
      {
        name: "TagReference",
        description: "タグを参照",
        parameter: Maybe.Just(customType.TagReference),
      },
      {
        name: "FunctionCall",
        description: "関数呼び出し",
        parameter: Maybe.Just(customType.functionCall),
      },
      {
        name: "Lambda",
        description: "ラムダ",
        parameter: Maybe.Just(Type.List(customType.lambdaBranch)),
      },
    ]),
  },
  {
    name: name.evaluatedExpr,
    description: "評価しきった式",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "Kernel",
        description: "Definyだけでは表現できない式",
        parameter: Maybe.Just(customType.kernelExpr),
      },
      {
        name: "Int32",
        description: "32bit整数",
        parameter: Maybe.Just(Type.Int32),
      },
      {
        name: "LocalPartReference",
        description: "ローカルパーツの参照",
        parameter: Maybe.Just(customType.localPartReference),
      },
      {
        name: "TagReference",
        description: "タグを参照",
        parameter: Maybe.Just(customType.TagReference),
      },
      {
        name: "Lambda",
        description: "ラムダ",
        parameter: Maybe.Just(Type.List(customType.lambdaBranch)),
      },
      {
        name: "KernelCall",
        description: "内部関数呼び出し",
        parameter: Maybe.Just(customType.kernelCall),
      },
    ]),
  },
  {
    name: name.kernelCall,
    description: "複数の引数が必要な内部関数の部分呼び出し",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "kernel",
        description: "関数",
        type: customType.kernelExpr,
      },
      {
        name: "expr",
        description: "呼び出すパラメーター",
        type: customType.evaluatedExpr,
      },
    ]),
  },
  {
    name: name.kernelExpr,
    description: "Definyだけでは表現できない式",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "Int32Add",
        description: "32bit整数を足す関数",
        parameter: Maybe.Nothing(),
      },
      {
        name: "Int32Sub",
        description: "32bit整数を引く関数",
        parameter: Maybe.Nothing(),
      },
      {
        name: "Int32Mul",
        description: "32bit整数をかける関数",
        parameter: Maybe.Nothing(),
      },
    ]),
  },
  {
    name: name.localPartReference,
    description: "ローカルパスの参照を表す",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "partId",
        description: "ローカルパスが定義されているパーツのID",
        type: idAndToken.partId,
      },
      {
        name: "localPartId",
        description: "ローカルパーツID",
        type: idAndToken.localPartId,
      },
    ]),
  },
  {
    name: name.tagReference,
    description: "タグの参照を表す",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "typePartId",
        description: "型ID",
        type: idAndToken.typePartId,
      },
      {
        name: "tagId",
        description: "タグID",
        type: idAndToken.tagId,
      },
    ]),
  },
  {
    name: name.functionCall,
    description: "関数呼び出し",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "function",
        description: "関数",
        type: customType.expr,
      },
      {
        name: "parameter",
        description: "パラメーター",
        type: customType.expr,
      },
    ]),
  },
  {
    name: name.lambdaBranch,
    description: "ラムダのブランチ. Just x -> data x のようなところ",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "condition",
        description: "入力値の条件を書くところ. Just x",
        type: customType.condition,
      },
      {
        name: "description",
        description: "ブランチの説明",
        type: Type.String,
      },
      {
        name: "localPartList",
        description: "",
        type: Type.List(customType.branchPartDefinition),
      },
      {
        name: "expr",
        description: "式",
        type: customType.expr,
      },
    ]),
  },
  {
    name: name.condition,
    description: "ブランチの式を使う条件",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "ByTag",
        description: "タグ",
        parameter: Maybe.Just(customType.conditionTag),
      },
      {
        name: "ByCapture",
        description: "キャプチャパーツへのキャプチャ",
        parameter: Maybe.Just(customType.conditionCapture),
      },
      {
        name: "Any",
        description: "_ すべてのパターンを通すもの",
        parameter: Maybe.Nothing(),
      },
      {
        name: "Int32",
        description: "32bit整数の完全一致",
        parameter: Maybe.Just(Type.Int32),
      },
    ]),
  },
  {
    name: name.conditionTag,
    description: "タグによる条件",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "tag",
        description: "タグ",
        type: idAndToken.tagId,
      },
      {
        name: "parameter",
        description: "パラメーター",
        type: Type.Maybe(customType.condition),
      },
    ]),
  },
  {
    name: name.conditionCapture,
    description: "キャプチャパーツへのキャプチャ",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "name",
        description: "キャプチャパーツの名前",
        type: Type.String,
      },
      {
        name: "localPartId",
        description: "ローカルパーツId",
        type: idAndToken.localPartId,
      },
    ]),
  },
  {
    name: name.branchPartDefinition,
    description: "ラムダのブランチで使えるパーツを定義する部分",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "localPartId",
        description: "ローカルパーツID",
        type: idAndToken.localPartId,
      },
      {
        name: "name",
        description: "ブランチパーツの名前",
        type: Type.String,
      },
      {
        name: "description",
        description: "ブランチパーツの説明",
        type: Type.String,
      },
      {
        name: "type",
        description: "ローカルパーツの型",
        type: customType.type,
      },
      {
        name: "expr",
        description: "ローカルパーツの式",
        type: customType.expr,
      },
    ]),
  },
  {
    name: name.evaluateExprError,
    description: "評価したときに失敗した原因を表すもの",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Sum([
      {
        name: "NeedPartDefinition",
        description: "式を評価するには,このパーツの定義が必要だと言っている",
        parameter: Maybe.Just(idAndToken.partId),
      },
      {
        name: "NeedSuggestionPart",
        description: "式を評価するために必要なSuggestionPartが見つからない",
        parameter: Maybe.Just(Type.Int32),
      },
      {
        name: "Blank",
        description: "計算結果にblankが含まれている",
        parameter: Maybe.Nothing(),
      },
      {
        name: "CannotFindLocalPartDefinition",
        description: "ローカルパーツの定義を見つけることができなかった",
        parameter: Maybe.Just(customType.localPartReference),
      },
      {
        name: "TypeError",
        description: "型が合わない",
        parameter: Maybe.Just(customType.typeError),
      },
      {
        name: "NotSupported",
        description: "まだサポートしていないものが含まれている",
        parameter: Maybe.Nothing(),
      },
    ]),
  },
  {
    name: name.typeError,
    description: "型エラー",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "message",
        description: "型エラーの説明",
        type: Type.String,
      },
    ]),
  },
  {
    name: name.evalParameter,
    description: "評価する上で必要なソースコード",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "partList",
        description: "パーツのリスト",
        type: Type.List(
          customType.idAndData(idAndToken.partId, customType.part)
        ),
      },
      {
        name: "typePartList",
        description: "型パーツのリスト",
        type: Type.List(
          customType.idAndData(idAndToken.typePartId, customType.typePart)
        ),
      },
      {
        name: "changeList",
        description: "変更点",
        type: Type.List(customType.change),
      },
      {
        name: "expr",
        description: "評価してほしい式",
        type: customType.suggestionExpr,
      },
    ]),
  },
  {
    name: name.createProjectParameter,
    description: "プロジェクト作成時に必要なパラメーター",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "accessToken",
        description: "プロジェクトを作るときのアカウント",
        type: idAndToken.accessToken,
      },
      {
        name: "projectName",
        description: "プロジェクト名",
        type: Type.String,
      },
    ]),
  },
  {
    name: name.createIdeaParameter,
    description: "アイデアを作成時に必要なパラメーター",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "accessToken",
        description: "プロジェクトを作るときのアカウント",
        type: idAndToken.accessToken,
      },
      {
        name: "ideaName",
        description: "アイデア名",
        type: Type.String,
      },
      {
        name: "projectId",
        description: "対象のプロジェクトID",
        type: idAndToken.projectId,
      },
    ]),
  },
  {
    name: name.addCommentParameter,
    description: "アイデアにコメントを追加するときに必要なパラメーター",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "accessToken",
        description: "プロジェクトを作るときのアカウント",
        type: idAndToken.accessToken,
      },
      {
        name: "ideaId",
        description: "コメントを追加するアイデア",
        type: idAndToken.ideaId,
      },
      {
        name: "comment",
        description: "コメント本文",
        type: Type.String,
      },
    ]),
  },
  {
    name: name.addSuggestionParameter,
    description: "提案を作成するときに必要なパラメーター",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "accessToken",
        description: "提案を作成するアカウント",
        type: idAndToken.accessToken,
      },
      {
        name: "ideaId",
        description: "提案に関連付けられるアイデア",
        type: idAndToken.ideaId,
      },
    ]),
  },
  {
    name: name.updateSuggestionParameter,
    description: "提案を更新するときに必要なパラメーター",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "accessToken",
        description: "提案を更新するアカウント",
        type: idAndToken.accessToken,
      },
      {
        name: "suggestionId",
        description: "書き換える提案",
        type: idAndToken.suggestionId,
      },
      {
        name: "name",
        description: "提案の名前",
        type: Type.String,
      },
      {
        name: "reason",
        description: "変更理由",
        type: Type.String,
      },
      {
        name: "changeList",
        description: "提案の変更",
        type: Type.List(customType.change),
      },
    ]),
  },
  {
    name: name.accessTokenAndSuggestionId,
    description: "提案を承認待ちにしたり許可したりするときなどに使う",
    typeParameterList: [],
    body: CustomTypeDefinitionBody.Product([
      {
        name: "accessToken",
        description: "アクセストークン",
        type: idAndToken.accessToken,
      },
      {
        name: "suggestionId",
        description: "SuggestionId",
        type: idAndToken.suggestionId,
      },
    ]),
  },
];