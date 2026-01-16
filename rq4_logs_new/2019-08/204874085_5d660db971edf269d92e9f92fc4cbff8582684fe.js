import React from 'react';
import '../css/contentbody.css';
class Contentbody extends React.Component{
    render(){
        return(


            <div>
            <div className='center'>
				This is Content
				</div>   
               <main>
	<article>
		<header>
			<h1>Article heading
				<span>Secondary heading</span>
			</h1>
			<ul className="meta">
				<li>UX</li>
				<li>UI</li>
				<li>Work</li>
			</ul>
			<div className="article-billboard">
				<img className="billboard-desktop" src="https://images.unsplash.com/photo-1536183801678-ecc5eaf42bf9?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=500&q=60" />
			</div>
			<div className="article-links">
				<a href="#" className="primary">Visit the live project</a>
			</div>
		</header>
		<div className="article-content">
			<div className="content-container-standard">
				<h2>Sub-heading</h2>
				<p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aliquam tincidunt scelerisque turpis, quis suscipit turpis aliquam eu. Phasellus pharetra urna vel ante dapibus, varius mattis felis ornare. Donec vel ullamcorper purus. Praesent suscipit in mi non consectetur. Sed pharetra fringilla tincidunt. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas.</p>
				<p>Nunc gravida eu purus et tristique. Pellentesque placerat, libero nec lacinia mattis, lectus leo imperdiet tellus, a hendrerit elit est a ante. Quisque ultricies, nunc ac ultrices ultrices, leo eros condimentum diam, ut suscipit diam ante at augue. Cras ultricies cursus tincidunt.</p>
				<h3>Sub-sub-heading over two lines</h3>
				<p>Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Proin ultrices metus vitae ultrices volutpat. Morbi eu nunc nisi. Phasellus convallis sem maximus viverra elementum. Morbi sit amet ipsum a arcu ultrices pretium ac quis ligula. Morbi auctor, urna nec tempor consectetur, lacus velit dictum mi, vitae placerat est ipsum vitae odio.</p>
				<ul className="colour-list">
					<li>Ante dapibus</li>
					<li>Varius mattis</li>
					<li>Felis ornare</li>
					<li>Ante dapibus</li>
					<li>Varius mattis</li>
					<li>Felis ornare</li>
					<li>Ante dapibus</li>
					<li>Varius mattis</li>
					<li>Felis ornare</li>
				</ul>
				<h2>Research & design</h2>
				<p><strong>Quisque ullamcorper venenatis pretium.</strong> Fusce nec elit scelerisque, finibus arcu quis, maximus nisl. Nunc eget magna sit amet augue lacinia rutrum sit amet sit amet purus. Donec volutpat, <a href="#">elit sit amet porttitor</a> aliquam, urna libero pretium ligula, a fermentum leo augue a dui. Ut interdum ipsum mattis dapibus ornare. Etiam ac dui id felis feugiat volutpat. Suspendisse tristique vestibulum purus eget tincidunt.</p>
				<p>Praesent suscipit in mi non consectetur. Sed pharetra fringilla tincidunt. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas.</p>
				<ul>
					<li>Ante dapibus</li>
					<li>Varius mattis</li>
					<li>Felis ornare</li>
				</ul>
				<img className="profile" src="https://images.unsplash.com/photo-1540959733332-eab4deabeeaf?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=1071&q=80" />
            
			</div>
		</div>
	</article>
</main>
<footer>
	<p className="copyright">Copyright notice</p>
</footer>
            </div>
        )
    }
}

export default Contentbody;