import { SuperRequestable, requestable } from '../../index';

export class Server {
	/**
	 * Starts the server using the preconfigured .env Redis URL
	 */
	public static start(): void {
		SuperRequestable.start(process.env.REDIS_URL!);
	}

	// ------------------------------------------------------
	// GET
	// ------------------------------------------------------
	@requestable('GET')
	public noEchoStringGET(): string {
		return 'ok';
	}

	@requestable('GET')
	public noEchoNumberGET(): number {
		return 200;
	}

	@requestable('GET')
	public noEchoObjectGET(): { num: number, str: string } {
		return {
			num: 200,
			str: 'ok'
		}
	}

	@requestable('GET')
	public noEchoSimpleErrorGET() {
		throw new Error(`cannot divide by zero!`);
	}

	@requestable('GET', true)
	public echoSimpleErrorGET() {
		throw new Error(`cannot divide by zero!`);
	}

	@requestable('GET', true)
	public echoArg(myNum: number, myStr: string, myObj: any) {
		return {
			num: myNum + 1,
			str: myStr + ' world',
			obj: {
				content: myObj.content + ', hello!'
			}
		}
	}

	private async sleep(ms: number): Promise<void> {
		return new Promise(r => setTimeout(r, ms));
	}

	private async wait(): Promise<void> {
		await this.sleep(Math.random() * 500 + 10);
	}

	@requestable('GET', true)
	public async parallel1(): Promise<number> {
		await this.wait();
		return 1;
	}

	@requestable('GET', true)
	public async parallel2(): Promise<number> {
		await this.wait();
		return 2;
	}

	@requestable('GET', true)
	public async parallel3(): Promise<number> {
		await this.wait();
		return 3;
	}

	@requestable('GET', true)
	public async parallel4(): Promise<number> {
		await this.wait();
		return 4;
	}

	@requestable('GET')
	public async parallelN(n: number): Promise<number> {
		await this.wait();
		return n;
	}

	// ------------------------------------------------------
	// POST
	// ------------------------------------------------------
	@requestable('POST')
	public noEchoStringPOST(): string {
		return 'ok';
	}

	@requestable('POST')
	public noEchoNumberPOST(): number {
		return 200;
	}

	@requestable('POST')
	public noEchoObjectPOST(): { num: number, str: string } {
		return {
			num: 200,
			str: 'ok'
		}
	}
}