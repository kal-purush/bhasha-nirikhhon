import navbar from "../styles/componentstyles/navbar.module.scss";
import quill_hamburger from "../public/images/quill_hamburger.svg";
import Image from "next/image";
import Link from "next/link";
import text from "../pages/api/navbarcontent.json";

export const Navbar0 = () => {
	return (
		<div className="container-fluid mb-1">
			<div className="row">
				{text.landingpage.map((navbar) => {
					return (
						<div
							className="col-sm-12 col-md-12 col-lg-12 p-0 m-0"
							key={navbar.id}>
							<Navbar {...navbar}></Navbar>
						</div>
					);
				})}
			</div>
		</div>
	);
};

export const Navbar1 = () => {
	return (
		<div className="container-fluid">
			<div className="row">
				{text.portfolio.map((navbar) => {
					return (
						<div
							className="col-sm-12 col-md-12 col-lg-12 p-0 m-0 "
							key={navbar.id}>
							<Navbar {...navbar}></Navbar>
						</div>
					);
				})}
			</div>
		</div>
	);
};

export const Navbar2 = () => {
	return (
		<div className="container-fluid ">
			<div className="row">
				{text.portfolios.map((navbar) => {
					return (
						<div className="col-sm-12 col-md-12 col-lg-12 p-0 " key={navbar.id}>
							<Navbar {...navbar}></Navbar>
						</div>
					);
				})}
			</div>
		</div>
	);
};

const Navbar = ({ heading }) => {
	return (
		<>
			<nav className="navbar navbar-dark bg-dark">
				<div className="container-fluid">
					<Link className={`navbar-brand ${navbar.logofont}`} href="/">
						Titilayo
					</Link>
					<h6 className="text-white nav-link">{heading}</h6>
					<Image
						src={quill_hamburger}
						type="button"
						data-bs-toggle="offcanvas"
						data-bs-target="#offcanvasDarkNavbar"
						aria-controls="offcanvasDarkNavbar"
						width={40}
						alt=""
					/>

					<div
						className="offcanvas offcanvas-end text-bg-dark"
						tabindex="-1"
						id="offcanvasDarkNavbar"
						aria-labelledby="offcanvasDarkNavbarLabel">
						<div className="offcanvas-header">
							<Link
								href="/"
								className={`offcanvas-title ${navbar.logofont}`}
								id="offcanvasDarkNavbarLabel">
								Titilayo
							</Link>
							<button
								type="button"
								className="btn-close btn-close-white"
								data-bs-dismiss="offcanvas"
								aria-label="Close"></button>
						</div>
						<div className="offcanvas-body">
							<ul className="navbar-nav justify-content-end flex-grow-1 pe-3">
								<li className="nav-item">
									<Link
										className="nav-link active"
										aria-current="page"
										href="/">
										Home
									</Link>
								</li>
								<li className="nav-item">
									<a className="nav-link active" href="/">
										Consulting
									</a>
								</li>
								<li className="nav-item">
									<a className="nav-link active" href="/">
										Entrepreneur
									</a>
								</li>
								<li className="nav-item">
									<Link className="nav-link active" href="/">
										Books
									</Link>
								</li>
								<li className="nav-item">
									<a className="nav-link active" href="/">
										Podcast
									</a>
								</li>
								<li className="nav-item">
									<a className="nav-link active" href="/">
										Musing
									</a>
								</li>
								<li className="nav-item">
									<Link className="nav-link active" href="/contact">
										Contact
									</Link>
								</li>
							</ul>
						</div>
					</div>
				</div>
			</nav>
		</>
	);
};

export default Navbar;