import {ElementRef} from '@angular/core';

export class Canvas {
  private canvas;
  private width;
  private height;
  private commands;
  public currentX;
  public currentY;
  private shouldDraw;

  constructor(public element: nativeElement) {
    this.canvas = this.element.getContext('2d');
    this.width = this.element.width;
    this.height = this.element.height;
    this.currentX = this.width / 2;
    this.currentY = this.height / 2;
    this.commands = [];
    this.shouldDraw = false;
  }


  public execute(command) {
    this.commands.push(command);

    this.currentX = this.width / 2;
    this.currentY = this.height / 2;

    let that = this;
    this.commands.forEach(function (command) {
      that.draw(command);
    });

    this.drawCross(this.currentX, this.currentY);
  }

  private draw(command) {
    console.log('Drawing: ' + command.toString());

    if (command.target === 'background') {
      this.drawRectangle(command.value, 0, 0, this.width, this.height);
    }

    if (command.target === 'stroke') {
      if (command.value === 'up') {
        let oldY = this.currentY;

        if (this.currentY - 5 > 0)
          this.currentY -= 5;
        this.drawLine(this.currentX, oldY, this.currentX, this.currentY, this.shouldDraw);
      }

      if (command.value === 'down') {
        let oldY = this.currentY;

        if (this.currentY + 5 < this.height)
          this.currentY += 5;
        this.drawLine(this.currentX, oldY, this.currentX, this.currentY, this.shouldDraw);
      }

      if (command.value === 'left') {
        let oldX = this.currentX;

        if (this.currentX - 5 > 0)
          this.currentX -= 5;
        this.drawLine(oldX, this.currentY, this.currentX, this.currentY, this.shouldDraw);
      }

      if (command.value === 'right') {
        let oldX = this.currentX;

        if (this.currentX + 5 < this.width)
          this.currentX += 5;
        this.drawLine(oldX, this.currentY, this.currentX, this.currentY, this.shouldDraw);
      }
    }

    if (command.target === 'draw') {
      this.shouldDraw = command.value === 'on';
    }
  }

  private drawRectangle(color, minX, minY, maxX, maxY) {
    this.canvas.fillStyle = color;
    this.canvas.fillRect(minX, minY, maxX, maxY);
  }

  private drawCross(posX, posY) {
    this.drawLine(posX - 5, posY, posX + 5, posY, true);
    this.drawLine(posX, posY - 5, posX, posY + 5, true);
  }

  private drawLine(minX, minY, maxX, maxY, shouldDraw) {
    this.canvas.beginPath();
    this.canvas.moveTo(minX, minY);
    this.canvas.lineTo(maxX, maxY);

    if (shouldDraw)
      this.canvas.stroke();
  }
}