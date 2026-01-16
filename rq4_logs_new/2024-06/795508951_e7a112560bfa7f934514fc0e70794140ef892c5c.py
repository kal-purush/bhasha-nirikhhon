import pygame
import sys

pygame.init()

# Configurações do jogo
# --------------------------------------------------
velocidade_jogo = 60
tempodojogo = pygame.time.Clock()
largura, altura = 1250, 720
tela = pygame.display.set_mode((largura, altura))
chaoinicio = (0, altura)
chaofim = (largura, altura)
colisao_chao = (chaoinicio, chaofim)
vermelho = (200, 0, 0)
preto = (0, 0, 0)
verde = (0, 150, 0)
azul = (0, 0, 150)
# --------------------------------------------------

# Configurações do quadrado
# --------------------------------------------------
quadrado = pygame.Rect(50, 620, 20, 50)
quadrado_vel = 0
gravidade = 1.5
# --------------------------------------------------

# Inicializa a fonte
fonte = pygame.font.SysFont("Arial", 20)

# Configurações do círculo
# --------------------------------------------------
circulo = [1200, 650] #posição do circulo
circulo_vel = 5
raio = 10
pontos = 0
recorde = 0
novorecorde = 0
# --------------------------------------------------

def dificuldade(pontos, circulo_vel):
    if pontos >= 10 and pontos < 20:
        circulo_vel += 0.1
        
    elif pontos >= 20 and pontos < 30:
        circulo_vel += 0.2
        
    elif pontos >= 30 and pontos < 40:
        circulo_vel += 0.3
        
    elif pontos >= 40 and pontos < 50:
        circulo_vel += 0.4
        
    elif pontos >= 50 and pontos < 60:
        circulo_vel += 0.5
        
    return circulo_vel


def colisao(circulo, quadrado):
    circulo_colisao = pygame.Rect(circulo[0] - raio, circulo[1] - raio, raio*2, raio*2)
    return quadrado.colliderect(circulo_colisao)

no_ar = False

rodando = True
while rodando:
    tela.fill(azul)
    for evento in pygame.event.get():
        if evento.type == pygame.QUIT:
            rodando = False
            pygame.quit()
            sys.exit()

        #futuros controles
        elif evento.type == pygame.KEYDOWN:
            if evento.key == pygame.K_SPACE and not no_ar:
                quadrado_vel = -20
                no_ar = True


    #movimentação do circulo
    circulo[0] -= circulo_vel
    if circulo[0] + raio < 0:
        circulo[0] = largura + raio
        pontos += 1
        recorde = pontos
        if novorecorde <= recorde:
            novorecorde = recorde
        

    circulo_vel = dificuldade(pontos, circulo_vel)
    
    
    
    quadrado.y += quadrado_vel
    quadrado_vel += gravidade

    if quadrado.y >= 620:
        quadrado.y = 620
        quadrado_vel = 0
        no_ar = False
    
    # Verificar colisão
    if colisao(circulo, quadrado):
        if novorecorde == recorde:
            novorecorde = recorde
        pontos = 0
        circulo_vel = 5
        circulo[0] = largura + raio  # Redefine a posição do círculo
        # lembrar de exibir uma mensagem de "Game Over"

    mostrar_pontos = fonte.render(f"Pontuação: {pontos}", True, verde)
    mostrar_velocidade = fonte.render(f"Velocidade: {circulo_vel:.1f}", True, vermelho)
    mostrar_recorde = fonte.render(f"Recorde: {novorecorde}", True, verde)
    tela.blit(mostrar_recorde,(100, 150))
    tela.blit(mostrar_velocidade,(100, 100))
    tela.blit(mostrar_pontos, (100, 50))

    pygame.draw.circle(tela, vermelho, circulo, raio)
    pygame.draw.rect(tela, preto, quadrado)
    pygame.draw.line(tela, verde, chaoinicio, chaofim, 100)

    pygame.display.flip()
    tempodojogo.tick(velocidade_jogo)