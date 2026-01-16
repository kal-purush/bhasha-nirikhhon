/**
 * 开发环境配置（webpack-dev-server）
 * Created by AoSnow on 2017/6/19.
 */

const Path = require( 'path' );

const ROOT = Path.resolve( __dirname, '../' );
const SRC_DIR = Path.resolve( ROOT, 'src' );
const DIST_DIR = Path.resolve( ROOT, 'dist' );
const TPL_DIR = Path.resolve( SRC_DIR, 'tpl' );

const loader = { rules: [] };
const plugins = [];

// CSS内容提取成独立文件
const scssExtractor = require( './plugins/ExtractTextWebpackPlugin' );

// 组织加载器列表
loader.rules.push(
	require( './rules/scss' )( scssExtractor ),
	require( './rules/fonts' ),
	require( './rules/image' ),
	require( './rules/html' ),
	require( './rules/typescript' )
);

// 组织插件列表
plugins.push(
	scssExtractor,
	require( './plugins/CommonsChunkPlugin' )
);

// 模板页面列表
const pages = require( './plugins/HtmlWebpackPlugin' )( TPL_DIR );
Array.prototype.push.apply( plugins, pages );

module.exports = {
	entry: {
		app: './src/app.tsx',
		vendor: [ 'react', 'react-dom' ]
	},
	output: {
		filename: "js/[name].js",
		path: DIST_DIR
		//publicPath: "../"
	},
	externals: {
		"react": "React",
		"react-dom": "ReactDOM"
	},
	module: loader,
	plugins: plugins,
	resolve: {
		extensions: [ '.scss', '.js', '.ts', '.tsx', '.json' ]
	},
	devServer: {
		// 内容根目录
		contentBase: './dist',
		// 告诉服务器监视那些通过 devServer.contentBase 选项提供的文件
		// 文件改动将触发整个页面重新加载
		// watchContentBase: true,
		// 是否启用 gzip 压缩
		compress: true,
		// 服务端口
		port: 8600,
		historyApiFallback: true,
		// 启用 webpack 的模块热替换特性
		hot: true,
		// 内联模式启动，支持热更新
		inline: true,
		// 使用 WAMP 来配置 API 服务器
		// （如 /api/users 现在会被代理到请求 http://localhost:80/api/users）
		proxy: { "/api": "http://localhost:80" }
	}
};